use sqlx::SqlitePool;
use sqlx::types::Json;

use super::models::{Manifest, ManifestReferrer};
use crate::utils::manifest::OCIManifest;

pub struct ManifestRepository {
    db_ro: SqlitePool,
    db_rw: SqlitePool,
}

impl std::fmt::Debug for ManifestRepository {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManifestRepository").finish_non_exhaustive()
    }
}

impl ManifestRepository {
    pub fn new(db_ro: SqlitePool, db_rw: SqlitePool) -> Self {
        Self { db_ro, db_rw }
    }

    /// SELECT m.blob, m.json ->> 'mediaType', m.digest FROM manifest m WHERE m.digest = $1
    pub async fn find(&self, digest: &str) -> Result<Manifest, sqlx::Error> {
        sqlx::query_as!(
            Manifest,
            r#"
            SELECT m.blob, m.json ->> 'mediaType' as "media_type: String", m.digest
            FROM manifest m
            WHERE m.digest = $1
            "#,
            digest
        )
        .fetch_one(&self.db_ro)
        .await
    }

    /// INSERT INTO manifest (digest, json, blob) VALUES ($1, jsonb($2), $2) ON CONFLICT (digest) DO NOTHING
    pub async fn insert_or_ignore(&self, digest: &str, blob: &[u8]) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            INSERT INTO manifest (digest, json, blob)
            VALUES ($1, jsonb($2), $2)
            ON CONFLICT (digest) DO NOTHING
            "#,
            digest,
            blob
        )
        .execute(&self.db_rw)
        .await?;
        Ok(())
    }

    /// DELETE FROM manifest where digest = $1
    pub async fn delete(&self, digest: &str) -> Result<(), sqlx::Error> {
        sqlx::query!("DELETE FROM manifest where digest = $1", digest)
            .execute(&self.db_rw)
            .await?;
        Ok(())
    }

    /// SELECT json(m.json), m.digest, length(m.blob) FROM manifest m JOIN repo_blob_assoc ...
    pub async fn list_referrers(
        &self,
        repo: &str,
        digest: &str,
    ) -> Result<Vec<ManifestReferrer>, sqlx::Error> {
        sqlx::query_as!(
            ManifestReferrer,
            r#"
            SELECT json(m.json) as "content!: Json<OCIManifest>",
                m.digest,
                length(m.blob) as "size!: i64"
            FROM manifest m
            INNER JOIN repo_blob_assoc rba ON rba.manifest_digest = m.digest
            WHERE rba.repo_name = $1
                AND (m.json -> 'subject' ->> 'digest') = $2
            "#,
            repo,
            digest
        )
        .fetch_all(&self.db_ro)
        .await
    }

    /// SELECT DISTINCT manifest_digest FROM manifest_blob_assoc WHERE blob_digest = $1
    pub async fn list_manifests_using_blob(
        &self,
        blob_digest: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        sqlx::query_scalar!(
            r#"SELECT DISTINCT manifest_digest FROM manifest_blob_assoc WHERE blob_digest = $1"#,
            blob_digest
        )
        .fetch_all(&self.db_ro)
        .await
    }

    /// DELETE FROM manifest WHERE m is not worth keeping and nothing worth keeping refers to it
    ///
    /// A manifest that no tag points at keeps its layers pinned via `manifest_blob_assoc`, so
    /// `BlobRepository::list_orphaned_older_than_days` will never reclaim them. Tags are repointed
    /// (not deleted) whenever a tag is re-pushed, so this accumulates once per overwrite.
    ///
    /// `keep` is the set worth keeping. It is rooted at two things:
    ///
    /// - every manifest a tag points at, and
    /// - every manifest that has *not* gone cold, judged by its config blob (see below).
    ///
    /// Both roots matter. Seeding only from tags would treat a manifest kept by the age gate as
    /// worthless for the purpose of protecting what hangs off it, and so would collect the
    /// signature of an untagged-but-live image — the exact digest-pinned flow (build, push by
    /// digest, `cosign sign`) that OCI referrers exist to serve. That bites immediately rather
    /// than after `retention_secs`, because artifacts share one empty config blob
    /// (`application/vnd.oci.empty.v1+json`) whose age has nothing to do with the signature's.
    ///
    /// The roots are then closed over two edges: the children of an index (`$.manifests`), and
    /// any manifest whose `$.subject` points at something kept. The second edge is what protects
    /// OCI 1.1 referrers — signatures, SBOMs, attestations — which are pushed by digest and are
    /// untagged *by design*. `UNION` (not `UNION ALL`) makes a cyclic or self-referencing index
    /// terminate rather than spin.
    ///
    /// The subject edge reads from a materialized `referrer_edge` rather than joining `manifest`
    /// directly, which is what keeps the walk from going quadratic. SQLite will not use an index
    /// on `json_extract(json, '$.subject.digest')` inside a recursive term — it plans `SCAN`
    /// there even when the identical lookup outside the CTE plans `SEARCH ... USING INDEX` — so
    /// the direct join costs a full scan of `manifest`, with a JSON parse per row, for every
    /// member of `keep`. Precomputing the edges once sidesteps the planner: on a synthetic
    /// 20k-manifest, 5k-tag database the walk goes from 12.66s to 0.01s, with no index involved.
    ///
    /// The age gate exists because untagged does *not* mean unreachable to a client:
    /// `GET .../manifests/<digest>` still serves these, and digest-pinning is common under
    /// Kubernetes. `manifest` carries no timestamp of its own (dropped in
    /// `02_no_manifest_last_accessed.sql` as dead schema — nothing ever wrote it, so it only ever
    /// held a creation time), so age is taken from the manifest's config blob: it is unique per
    /// image, unlike layers which are shared with derived images and stay warm indefinitely. A
    /// client resolving the manifest by digest goes on to fetch the config, which bumps
    /// `last_accessed` — but only when it actually pulls, so a digest-pinned image already cached
    /// on every node will still go cold. `retention_secs` is how long that is tolerated.
    ///
    /// An index has no `$.config` and so can never enter `keep` by age; the second half of the
    /// delete predicate collects one once nothing it lists is left in `manifest`. That needs the
    /// children to have gone first, so callers must run this to a fixpoint — see
    /// `GcService::delete_untagged_manifests`. An index that shares a child with a still-kept
    /// manifest is deliberately retained: it is a single row pinning no blobs, and keeping it is
    /// the conservative choice.
    ///
    /// Selecting and then deleting in separate statements would leave a window in which a
    /// concurrent push tags a manifest this query has already condemned — and `tag`'s foreign key
    /// cascades, so the just-created tag would vanish with it. Re-pushing an existing image is
    /// `insert_or_ignore` plus `tag.upsert`, which is exactly that shape, so the window is on the
    /// normal path rather than an exotic one. Deleting in one statement puts the decision under
    /// the same write lock as the push: either the push lands first and the manifest is kept, or
    /// the delete lands first and the push fails its foreign key loudly.
    ///
    /// Returns the number of manifests deleted.
    pub async fn delete_untagged_older_than(
        &self,
        retention_secs: i64,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query!(
            r#"
            WITH RECURSIVE
                -- Materialized so the subject edge joins against the handful of manifests that
                -- actually carry a subject, rather than re-scanning every manifest for each
                -- member of `keep`. MATERIALIZED is load-bearing here, not decoration.
                referrer_edge (child, parent) AS MATERIALIZED (
                    SELECT digest, json_extract(json, '$.subject.digest')
                    FROM manifest
                    WHERE json_extract(json, '$.subject.digest') IS NOT NULL
                ),
                keep (digest) AS (
                    SELECT manifest_digest FROM tag
                    UNION
                    SELECT m.digest
                    FROM manifest m
                    JOIN blob b ON b.digest = json_extract(m.json, '$.config.digest')
                    WHERE b.last_accessed >= unixepoch('now') - $1
                    UNION
                    SELECT json_extract(je.value, '$.digest')
                    FROM keep k
                    JOIN manifest m ON m.digest = k.digest
                    JOIN json_each(json_extract(m.json, '$.manifests')) je
                    WHERE json_extract(je.value, '$.digest') IS NOT NULL
                    UNION
                    SELECT e.child
                    FROM keep k
                    JOIN referrer_edge e ON e.parent = k.digest
                )
            DELETE FROM manifest
            WHERE NOT EXISTS (SELECT 1 FROM keep k WHERE k.digest = manifest.digest)
                AND (
                    -- Aged out. Coldness is already implied by falling out of `keep`; this only
                    -- asks whether the manifest was ageable at all. A manifest whose config blob
                    -- was never stored (a foreign config, say) has no clock, so it is left alone
                    -- rather than collected on the strength of a test that never ran.
                    EXISTS (
                        SELECT 1 FROM blob b
                        WHERE b.digest = json_extract(manifest.json, '$.config.digest')
                    )
                    -- Or: an index with nothing left to point at.
                    OR (
                        json_extract(manifest.json, '$.manifests') IS NOT NULL
                        AND NOT EXISTS (
                            SELECT 1
                            FROM json_each(json_extract(manifest.json, '$.manifests')) je
                            JOIN manifest child ON child.digest = json_extract(je.value, '$.digest')
                        )
                    )
                )
            "#,
            retention_secs
        )
        .execute(&self.db_rw)
        .await?;

        Ok(result.rows_affected())
    }
}
