use std::sync::Arc;

use tokio::time::{self, Duration};

use crate::TrowConfig;
use crate::file_storage::FileStorage;
use crate::repositories::Repositories;
use crate::services::Error;

#[derive(Debug)]
pub struct GcService {
    repos: Arc<Repositories>,
    storage: Arc<FileStorage>,
    config: Arc<TrowConfig>,
}

impl GcService {
    pub fn new(
        repos: Arc<Repositories>,
        storage: Arc<FileStorage>,
        config: Arc<TrowConfig>,
    ) -> Self {
        Self {
            repos,
            storage,
            config,
        }
    }

    /// Blocks forever, running the GC loop on a 10-minute interval.
    pub async fn watchdog(self: Arc<Self>) {
        let mut interval = time::interval(Duration::from_secs(600));
        loop {
            interval.tick().await;
            if let Err(e) = self.run_once().await {
                tracing::error!("Could not make room: {e}");
            }
        }
    }

    /// Runs one GC pass; safe to call manually (used in tests).
    pub async fn run_once(&self) -> Result<(), Error> {
        let space_to_reclaim = self.compute_space_to_reclaim().await?;

        let mut space_reclaimed = 0;
        space_reclaimed += self.delete_stale_uploads().await?;
        self.delete_untagged_manifests().await?;
        space_reclaimed += self.delete_orphan_blobs().await?;
        if let Some(space_required) = space_to_reclaim {
            space_reclaimed += self
                .delete_old_proxied_images(space_required.saturating_sub(space_reclaimed))
                .await?;
            if space_reclaimed < space_required {
                tracing::warn!(
                    needed = bytes_humanstring(space_required),
                    "Could not reclaim enough space"
                )
            }
        }
        if space_reclaimed > 0 {
            tracing::info!(
                reclaimed = bytes_humanstring(space_reclaimed),
                "Total space reclaimed"
            );
        }
        Ok(())
    }

    async fn compute_space_to_reclaim(&self) -> Result<Option<usize>, Error> {
        let Some(limit) = self.config.config_file.registry_proxies.max_size else {
            return Ok(None);
        };
        let blobs = self.repos.blob.sum_size().await?;
        let uploads = self.repos.blob_upload.sum_offset().await?;
        let space_taken = blobs + uploads;
        let space_available = (limit.bytes() as f64 * 0.8) as usize;
        let needed = space_taken.saturating_sub(space_available);
        Ok((needed > 0).then_some(needed))
    }

    pub async fn delete_stale_uploads(&self) -> Result<usize, Error> {
        let mut bytes_reclaimed = 0;
        let stale = self.repos.blob_upload.list_stale_older_than_days().await?;
        for upload in stale {
            self.repos.blob_upload.delete(&upload.uuid).await?;
            self.storage.delete_upload(&upload.uuid).await?;
            bytes_reclaimed += upload.offset as usize;
        }
        if bytes_reclaimed > 0 {
            tracing::info!(
                reclaimed = bytes_humanstring(bytes_reclaimed),
                "Reclaimed space by deleting stale uploads"
            )
        }
        Ok(bytes_reclaimed)
    }

    /// Deletes manifests nothing worth keeping refers to, unpinning their blobs for
    /// `delete_orphan_blobs`.
    ///
    /// Returns the number of manifests deleted, not bytes: a manifest holds no storage of its own,
    /// and the space its layers were pinning is attributed to `delete_orphan_blobs`.
    ///
    /// Runs to a fixpoint: an index is only collectable once nothing it lists is left, so deleting
    /// the children of an orphaned multi-arch image is what makes the index itself collectable on
    /// the next pass. Each pass deletes at least one row from a finite table, so this terminates;
    /// `MAX_PASSES` is only a guard against a delete that somehow fails to remove its row.
    pub async fn delete_untagged_manifests(&self) -> Result<u64, Error> {
        const MAX_PASSES: usize = 16;
        let retention_secs = self
            .config
            .config_file
            .garbage_collection
            .untagged_manifest_retention_secs();

        let mut deleted = 0;
        let mut settled = false;
        for _ in 0..MAX_PASSES {
            let pass_deleted = self
                .repos
                .manifest
                .delete_untagged_older_than(retention_secs)
                .await?;
            // A pass that deletes nothing is the fixpoint. Checking the *outcome* rather than the
            // pass number keeps a run that happens to finish on the last pass from being reported
            // as a failure to settle.
            if pass_deleted == 0 {
                settled = true;
                break;
            }
            deleted += pass_deleted;
        }
        if !settled {
            tracing::warn!("Untagged manifest collection did not settle in {MAX_PASSES} passes");
        }
        if deleted > 0 {
            tracing::info!(deleted, "Deleted untagged manifests");
        }
        Ok(deleted)
    }

    pub async fn delete_orphan_blobs(&self) -> Result<usize, Error> {
        let mut bytes_reclaimed = 0;
        let blobs = self.repos.blob.list_orphaned_older_than_days().await?;
        for blob in blobs {
            self.repos.blob.delete(&blob.digest).await?;
            self.storage.delete_blob(&blob.digest).await?;
            bytes_reclaimed += blob.size as usize;
        }
        if bytes_reclaimed > 0 {
            tracing::info!(
                reclaimed = bytes_humanstring(bytes_reclaimed),
                "Reclaimed space by deleting orphaned blobs"
            )
        }
        Ok(bytes_reclaimed)
    }

    pub async fn delete_old_proxied_images(&self, space_needed: usize) -> Result<usize, Error> {
        let mut bytes_reclaimed = 0;
        let mut proxied = self.repos.blob.list_proxied_older_than_days().await?;

        while bytes_reclaimed < space_needed {
            let Some(blob) = proxied.pop() else {
                return Ok(bytes_reclaimed);
            };

            let manifests = self
                .repos
                .manifest
                .list_manifests_using_blob(&blob.digest)
                .await?;
            for md in manifests {
                self.repos.manifest.delete(&md).await?;
            }
            self.repos.blob.delete(&blob.digest).await?;
            self.storage.delete_blob(&blob.digest).await?;
            bytes_reclaimed += blob.size as usize;
        }
        if bytes_reclaimed > 0 {
            tracing::info!(
                reclaimed = bytes_humanstring(bytes_reclaimed),
                "Reclaimed space by deleting proxied blobs"
            )
        }
        Ok(bytes_reclaimed)
    }
}

fn bytes_humanstring(bytes: usize) -> String {
    size::Size::from_bytes(bytes).to_string()
}

#[cfg(test)]
mod tests {
    use crate::test_utilities;
    use crate::test_utilities::test_temp_dir;

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_old_proxied_images() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:test1', 100, strftime('%s', 'now', '-3 days')),
                   ('sha256:test2', 175, strftime('%s', 'now', '-3 day')),
                   ('sha256:test3', 300, strftime('%s', 'now', '-2 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        sqlx::query!(
            r#"
            INSERT INTO repo_blob_assoc (repo_name, blob_digest)
            VALUES ('f/test_repo1', 'sha256:test1'),
                   ('f/test_repo3', 'sha256:test3')
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let dummy_manifest =
            r#"{"config":{"digest":"sha256:test2"},"layers":[{"digest":"sha256:test3"}]}"#
                .as_bytes();
        sqlx::query!(
            r#"
            INSERT INTO manifest (digest, blob, json)
            VALUES ('sha256:test_manifest', $1, jsonb($1))
            "#,
            dummy_manifest
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let space_needed = 250;
        let result = state
            .services
            .gc
            .delete_old_proxied_images(space_needed)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 275);

        let repo_blob_assocs = sqlx::query_scalar!(r#"SELECT repo_name FROM repo_blob_assoc"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        assert_eq!(&repo_blob_assocs, &["f/test_repo3"]);

        let manifests = sqlx::query_scalar!(r#"SELECT digest FROM manifest"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        assert!(manifests.is_empty());
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_orphan_blobs() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:test1', 28, strftime('%s', 'now', '-3 days')),
                   ('sha256:test2', 200, strftime('%s', 'now', '-3 days')),
                   ('sha256:test3', 155, strftime('%s', 'now', '-3 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let dummy_manifest =
            r#"{"config":{"digest":"sha256:test1"},"layers":[{"digest":"sha256:test3"}]}"#
                .as_bytes();
        sqlx::query!(
            r#"
            INSERT INTO manifest (digest, blob, json)
            VALUES ('sha256:test_manifest1', $1, jsonb($1))
            "#,
            dummy_manifest
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let result = state.services.gc.delete_orphan_blobs().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 200);

        let blobs = sqlx::query_scalar!(r#"SELECT digest FROM blob"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        assert_eq!(blobs.len(), 2);
        assert!(blobs.contains(&"sha256:test1".to_string()));
        assert!(blobs.contains(&"sha256:test3".to_string()));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_stale_uploads() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob_upload (uuid, offset, updated_at, repo)
            VALUES ('test-uuid-1', 100, unixepoch('now', '-2 days'), 'testrepo'),
                   ('test-uuid-2', 200, unixepoch('now', '-5 hours'), 'testrepo'),
                   ('test-uuid-3', 150, unixepoch('now', '-9 days'), 'testrepo')
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        // Goes through the default, which must be the same epoch form the staleness
        // comparison uses, not a CURRENT_TIMESTAMP datetime string.
        state
            .services
            .repos()
            .blob_upload
            .create("test-uuid-4", "testrepo")
            .await
            .unwrap();

        let result = state.services.gc.delete_stale_uploads().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 250);

        let mut uploads = sqlx::query_scalar!(r#"SELECT uuid FROM blob_upload"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        uploads.sort();
        assert_eq!(uploads, vec!["test-uuid-2", "test-uuid-4"]);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_stale_uploads_collects_untouched_uploads() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        state
            .services
            .repos()
            .blob_upload
            .create("never-touched", "testrepo")
            .await
            .unwrap();

        sqlx::query!(
            r#"UPDATE blob_upload SET updated_at = unixepoch('now', '-2 days') WHERE uuid = 'never-touched'"#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let result = state.services.gc.delete_stale_uploads().await;
        assert!(result.is_ok());

        let uploads = sqlx::query_scalar!(r#"SELECT uuid FROM blob_upload"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        assert!(
            uploads.is_empty(),
            "upload created but never written to was not collected"
        );
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_untagged_manifests() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:cfg_tagged', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:cfg_untagged', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:cfg_warm', 10, strftime('%s', 'now', '-1 hour')),
                   ('sha256:layer', 500, strftime('%s', 'now', '-30 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let tagged =
            r#"{"config":{"digest":"sha256:cfg_tagged"},"layers":[{"digest":"sha256:layer"}]}"#
                .as_bytes();
        let untagged =
            r#"{"config":{"digest":"sha256:cfg_untagged"},"layers":[{"digest":"sha256:layer"}]}"#
                .as_bytes();
        let warm =
            r#"{"config":{"digest":"sha256:cfg_warm"},"layers":[{"digest":"sha256:layer"}]}"#
                .as_bytes();

        for (digest, json) in [
            ("sha256:m_tagged", tagged),
            ("sha256:m_untagged", untagged),
            ("sha256:m_warm", warm),
        ] {
            sqlx::query!(
                r#"INSERT INTO manifest (digest, blob, json) VALUES ($1, $2, jsonb($2))"#,
                digest,
                json
            )
            .execute(state.services.repos().db_rw())
            .await
            .unwrap();
        }

        sqlx::query!(
            r#"
            INSERT INTO tag (tag, repo, manifest_digest)
            VALUES ('latest', 'testrepo', 'sha256:m_tagged')
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let result = state.services.gc.delete_untagged_manifests().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let manifests = sqlx::query_scalar!(r#"SELECT digest FROM manifest"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        assert_eq!(manifests.len(), 2);
        assert!(manifests.contains(&"sha256:m_tagged".to_string()));
        assert!(manifests.contains(&"sha256:m_warm".to_string()));

        let assocs = sqlx::query_scalar!(
            r#"SELECT manifest_digest FROM manifest_blob_assoc WHERE manifest_digest = 'sha256:m_untagged'"#
        )
        .fetch_all(state.services.repos().db_ro())
        .await
        .unwrap();
        assert!(assocs.is_empty(), "manifest_blob_assoc was not cascaded");
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_untagged_manifests_keeps_index_children() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:cfg_child', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:layer', 500, strftime('%s', 'now', '-30 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let child =
            r#"{"config":{"digest":"sha256:cfg_child"},"layers":[{"digest":"sha256:layer"}]}"#
                .as_bytes();
        let index = r#"{"manifests":[{"digest":"sha256:m_child"}]}"#.as_bytes();

        for (digest, json) in [("sha256:m_child", child), ("sha256:m_index", index)] {
            sqlx::query!(
                r#"INSERT INTO manifest (digest, blob, json) VALUES ($1, $2, jsonb($2))"#,
                digest,
                json
            )
            .execute(state.services.repos().db_rw())
            .await
            .unwrap();
        }

        sqlx::query!(
            r#"
            INSERT INTO tag (tag, repo, manifest_digest)
            VALUES ('latest', 'testrepo', 'sha256:m_index')
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let result = state.services.gc.delete_untagged_manifests().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0, "index child was collected");

        let manifests = sqlx::query_scalar!(r#"SELECT digest FROM manifest"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        assert_eq!(manifests.len(), 2);
    }

    /// OCI 1.1 referrers (signatures, SBOMs, attestations) are pushed by digest and are untagged
    /// by design, so tag-only reachability would reclaim every artifact attached to a live image.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_untagged_manifests_keeps_referrers_of_tagged_image() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:cfg_image', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:cfg_sig', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:cfg_orphan_sig', 10, strftime('%s', 'now', '-30 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let image = r#"{"config":{"digest":"sha256:cfg_image"}}"#.as_bytes();
        // Attached to the tagged image: must survive.
        let sig = r#"{"config":{"digest":"sha256:cfg_sig"},"subject":{"digest":"sha256:m_image"}}"#
            .as_bytes();
        // Attached to a manifest that is itself unreachable: must not survive.
        let orphan_sig =
            r#"{"config":{"digest":"sha256:cfg_orphan_sig"},"subject":{"digest":"sha256:m_gone"}}"#
                .as_bytes();

        for (digest, json) in [
            ("sha256:m_image", image),
            ("sha256:m_sig", sig),
            ("sha256:m_orphan_sig", orphan_sig),
        ] {
            sqlx::query!(
                r#"INSERT INTO manifest (digest, blob, json) VALUES ($1, $2, jsonb($2))"#,
                digest,
                json
            )
            .execute(state.services.repos().db_rw())
            .await
            .unwrap();
        }

        sqlx::query!(
            r#"
            INSERT INTO tag (tag, repo, manifest_digest)
            VALUES ('latest', 'testrepo', 'sha256:m_image')
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let deleted = state.services.gc.delete_untagged_manifests().await.unwrap();
        assert_eq!(deleted, 1);

        let mut manifests = sqlx::query_scalar!(r#"SELECT digest FROM manifest"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        manifests.sort();
        assert_eq!(
            manifests,
            vec!["sha256:m_image", "sha256:m_sig"],
            "a referrer of a tagged image was collected"
        );
    }

    /// A referrer must outlive its subject regardless of *why* the subject is kept. Rooting
    /// reachability at tags alone protects only referrers of tagged images, which drops the
    /// signature of an image pinned by digest — build, push by digest, `cosign sign`, never tag.
    /// The signature dies immediately rather than after the retention window, because artifacts
    /// share one empty config blob whose age says nothing about when the signature was pushed.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_untagged_manifests_keeps_referrers_of_untagged_warm_image() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:cfg_pinned', 10, unixepoch('now')),
                   ('sha256:cfg_empty', 2, unixepoch('now', '-30 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        // Untagged, but freshly pushed: the age gate is what keeps it.
        let pinned = r#"{"config":{"digest":"sha256:cfg_pinned"}}"#.as_bytes();
        // Its signature, sharing the long-cold empty config blob every OCI artifact uses.
        let sig =
            r#"{"config":{"digest":"sha256:cfg_empty"},"subject":{"digest":"sha256:m_pinned"}}"#
                .as_bytes();

        for (digest, json) in [("sha256:m_pinned", pinned), ("sha256:m_pinned_sig", sig)] {
            sqlx::query!(
                r#"INSERT INTO manifest (digest, blob, json) VALUES ($1, $2, jsonb($2))"#,
                digest,
                json
            )
            .execute(state.services.repos().db_rw())
            .await
            .unwrap();
        }

        let deleted = state.services.gc.delete_untagged_manifests().await.unwrap();
        assert_eq!(deleted, 0);

        let mut manifests = sqlx::query_scalar!(r#"SELECT digest FROM manifest"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        manifests.sort();
        assert_eq!(
            manifests,
            vec!["sha256:m_pinned", "sha256:m_pinned_sig"],
            "the signature of an untagged but still-live image was collected"
        );
    }

    /// Re-pushing a multi-arch tag orphans the previous index. It has no config blob to age, so it
    /// is only collectable once its children are gone — which takes a second pass.
    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_untagged_manifests_collects_orphaned_index() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(|_| {}, &dir).await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:cfg_old_amd64', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:cfg_old_arm64', 10, strftime('%s', 'now', '-30 days')),
                   ('sha256:cfg_new', 10, strftime('%s', 'now', '-1 hour'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let old_amd64 = r#"{"config":{"digest":"sha256:cfg_old_amd64"}}"#.as_bytes();
        let old_arm64 = r#"{"config":{"digest":"sha256:cfg_old_arm64"}}"#.as_bytes();
        let old_index =
            r#"{"manifests":[{"digest":"sha256:m_old_amd64"},{"digest":"sha256:m_old_arm64"}]}"#
                .as_bytes();
        let new_child = r#"{"config":{"digest":"sha256:cfg_new"}}"#.as_bytes();
        let new_index = r#"{"manifests":[{"digest":"sha256:m_new_child"}]}"#.as_bytes();

        for (digest, json) in [
            ("sha256:m_old_amd64", old_amd64),
            ("sha256:m_old_arm64", old_arm64),
            ("sha256:m_old_index", old_index),
            ("sha256:m_new_child", new_child),
            ("sha256:m_new_index", new_index),
        ] {
            sqlx::query!(
                r#"INSERT INTO manifest (digest, blob, json) VALUES ($1, $2, jsonb($2))"#,
                digest,
                json
            )
            .execute(state.services.repos().db_rw())
            .await
            .unwrap();
        }

        // 'latest' was repointed at the new index; the old one is now unreachable.
        sqlx::query!(
            r#"
            INSERT INTO tag (tag, repo, manifest_digest)
            VALUES ('latest', 'testrepo', 'sha256:m_new_index')
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let deleted = state.services.gc.delete_untagged_manifests().await.unwrap();
        assert_eq!(deleted, 3, "the orphaned index itself was left behind");

        let mut manifests = sqlx::query_scalar!(r#"SELECT digest FROM manifest"#)
            .fetch_all(state.services.repos().db_ro())
            .await
            .unwrap();
        manifests.sort();
        assert_eq!(manifests, vec!["sha256:m_new_child", "sha256:m_new_index"]);
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn test_delete_untagged_manifests_honours_configured_retention() {
        let dir = test_temp_dir!();
        let (state, _router) = test_utilities::trow_router(
            |cfg| {
                cfg.config_file
                    .garbage_collection
                    .untagged_manifest_retention_days = 30;
            },
            &dir,
        )
        .await;

        sqlx::query!(
            r#"
            INSERT INTO blob (digest, size, last_accessed)
            VALUES ('sha256:cfg', 10, strftime('%s', 'now', '-10 days'))
            "#
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        let image = r#"{"config":{"digest":"sha256:cfg"}}"#.as_bytes();
        sqlx::query!(
            r#"INSERT INTO manifest (digest, blob, json) VALUES ('sha256:m', $1, jsonb($1))"#,
            image
        )
        .execute(state.services.repos().db_rw())
        .await
        .unwrap();

        // 10 days cold, but the configured window is 30.
        let deleted = state.services.gc.delete_untagged_manifests().await.unwrap();
        assert_eq!(deleted, 0, "configured retention window was ignored");
    }
}
