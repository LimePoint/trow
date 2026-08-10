use serde::{Deserialize, Deserializer, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ImageValidationConfig {
    pub default: String,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct ConfigFile {
    #[serde(deserialize_with = "de_unwrap_or_default")]
    pub registry_proxies: RegistryProxiesConfig,
    pub image_validation: Option<ImageValidationConfig>,
    #[serde(default, deserialize_with = "de_unwrap_or_default")]
    pub garbage_collection: GarbageCollectionConfig,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct GarbageCollectionConfig {
    /// How long an untagged manifest must have gone untouched before it is reclaimed.
    ///
    /// Untagged manifests are still pullable by digest, so this is the window in which a
    /// digest-pinned deployment that never re-pulls (and therefore never warms the manifest's
    /// config blob) is protected. Raise it if you pin images by digest and roll nodes rarely.
    ///
    /// Must be at least 1. `0` is rejected rather than accepted, because it reads as "off" but
    /// would mean the opposite — collecting every untagged manifest on the next GC pass.
    #[serde(
        default = "default_untagged_manifest_retention_days",
        deserialize_with = "de_retention_days"
    )]
    pub untagged_manifest_retention_days: u32,
}

fn default_untagged_manifest_retention_days() -> u32 {
    7
}

fn de_retention_days<'de, D>(d: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let days = u32::deserialize(d)?;
    if days == 0 {
        return Err(serde::de::Error::custom(
            "untagged_manifest_retention_days must be at least 1: 0 does not disable collection, \
             it collects every untagged manifest on the next pass",
        ));
    }
    Ok(days)
}

impl Default for GarbageCollectionConfig {
    fn default() -> Self {
        Self {
            untagged_manifest_retention_days: default_untagged_manifest_retention_days(),
        }
    }
}

impl GarbageCollectionConfig {
    pub fn untagged_manifest_retention_secs(&self) -> i64 {
        i64::from(self.untagged_manifest_retention_days) * 86_400
    }
}

fn de_unwrap_or_default<'de, T, D>(d: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Deserialize::deserialize(d).map(|x: Option<_>| x.unwrap_or_default())
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct RegistryProxyConfigs(Vec<SingleRegistryProxyConfig>);

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegistryProxiesConfig {
    #[serde(default)]
    pub registries: RegistryProxyConfigs,
    #[serde(default)]
    pub offline: bool,
    #[serde(default)]
    pub max_size: Option<size::Size>,
}

fn normalize_path_prefix<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt
        .map(|s| s.trim_matches('/').to_string())
        .filter(|s| !s.is_empty()))
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct SingleRegistryProxyConfig {
    /// What containerd calls "namespace" (ghcr.io, docker.io, ...)
    /// This can be empty !!
    pub host: String,
    /// Optional path prefix for scoped credential matching.
    /// Allows different credentials for different projects on the same registry host.
    /// Example: "system" matches repos like "system/app", "system/worker".
    /// When multiple entries match the same host, the longest matching prefix wins.
    #[serde(default, deserialize_with = "normalize_path_prefix")]
    pub path_prefix: Option<String>,
    /// TODO: insecure currently means "use HTTP", we should also support self-signed TLS
    #[serde(default)]
    pub insecure: bool,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Default for RegistryProxiesConfig {
    fn default() -> Self {
        RegistryProxiesConfig {
            registries: RegistryProxyConfigs(Vec::new()),
            offline: true,
            max_size: None,
        }
    }
}

impl RegistryProxyConfigs {
    pub fn get_for<'a>(
        &'a self,
        registry: &str,
        repo: &str,
    ) -> Option<&'a SingleRegistryProxyConfig> {
        let matches = self.0.iter().filter_map(|proxy| {
            if proxy.host == registry {
                if let Some(proxy_prefix) = proxy.path_prefix.as_deref() {
                    // for prefix "org" match org/toto, not org_b/toto
                    if repo == proxy_prefix
                        || (repo.starts_with(proxy_prefix)
                            && repo.as_bytes().get(proxy_prefix.len()) == Some(&b'/'))
                    {
                        return Some((proxy_prefix.len(), proxy));
                    }
                } else {
                    return Some((0, proxy));
                }
            }
            None
        });
        matches
            .max_by_key(|(prefix_len, _)| *prefix_len)
            .map(|(_, registry)| registry)
    }
}

impl From<Vec<SingleRegistryProxyConfig>> for RegistryProxyConfigs {
    fn from(vec: Vec<SingleRegistryProxyConfig>) -> Self {
        RegistryProxyConfigs(vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_garbage_collection_defaults_when_absent() {
        // Whether the section is missing entirely or present but empty.
        for json in [
            r#"{"registry_proxies": null}"#,
            r#"{"registry_proxies": null, "garbage_collection": {}}"#,
        ] {
            let config: ConfigFile = serde_json::from_str(json).unwrap();
            assert_eq!(
                config.garbage_collection.untagged_manifest_retention_days, 7,
                "did not default: {json}"
            );
        }
    }

    /// 0 reads as "off" but would mean "collect every untagged manifest on the next pass", so it
    /// is rejected rather than quietly obeyed.
    #[test]
    fn test_garbage_collection_rejects_zero_retention() {
        let err = serde_json::from_str::<ConfigFile>(
            r#"{"registry_proxies": null,
                "garbage_collection": {"untagged_manifest_retention_days": 0}}"#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("must be at least 1"),
            "unhelpful error: {err}"
        );

        let config: ConfigFile = serde_json::from_str(
            r#"{"registry_proxies": null,
                "garbage_collection": {"untagged_manifest_retention_days": 1}}"#,
        )
        .unwrap();
        assert_eq!(
            config.garbage_collection.untagged_manifest_retention_days,
            1
        );
    }

    #[test]
    fn test_registry_proxy_deserialize_path_prefix() {
        let proxy_config: SingleRegistryProxyConfig =
            serde_json::from_str(r#"{"host": "registry.example.com", "path_prefix": "/org/sub/"}"#)
                .unwrap();
        assert_eq!(proxy_config.path_prefix.unwrap(), "org/sub");

        let proxy_config: SingleRegistryProxyConfig =
            serde_json::from_str(r#"{"host": "registry.example.com", "path_prefix": "/"}"#)
                .unwrap();
        assert_eq!(proxy_config.path_prefix, None);
    }

    #[test]
    fn test_registry_proxy_configs_path_prefix_longest_match_wins() {
        let config = RegistryProxiesConfig {
            registries: vec![
                SingleRegistryProxyConfig {
                    host: "registry.example.com".to_string(),
                    username: Some("default".to_string()),
                    ..Default::default()
                },
                SingleRegistryProxyConfig {
                    host: "registry.example.com".to_string(),
                    path_prefix: Some("org".to_string()),
                    username: Some("org-token".to_string()),
                    ..Default::default()
                },
                SingleRegistryProxyConfig {
                    host: "registry.example.com".to_string(),
                    path_prefix: Some("org/sub".to_string()),
                    username: Some("org-sub-token".to_string()),
                    ..Default::default()
                },
            ]
            .into(),
            ..Default::default()
        };
        // "org/sub/app" matches both, but "org/sub" is longer
        let proxy = config
            .registries
            .get_for("registry.example.com", "org/sub/app");
        assert_eq!(proxy.unwrap().username, Some("org-sub-token".to_string()));

        // "org/other" matches only "org"
        let proxy = config
            .registries
            .get_for("registry.example.com", "org/other");
        assert_eq!(proxy.unwrap().username, Some("org-token".to_string()));

        // no path_prefix match
        let proxy = config
            .registries
            .get_for("registry.example.com", "outta-this-world");
        assert_eq!(proxy.unwrap().username, Some("default".to_string()));

        // doesn't match path prefix across '/' boundary
        let proxy = config
            .registries
            .get_for("registry.example.com", "org_b/app");
        assert_eq!(proxy.unwrap().username, Some("default".to_string()));
    }
}
