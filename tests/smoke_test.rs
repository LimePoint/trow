#![cfg(test)]

mod common;

mod smoke_test {

    use std::net::TcpListener;
    use std::path::Path;
    use std::process::{Child, Command};
    use std::thread;
    use std::time::Duration;

    use axum::body::Body;
    use hyper::Request;
    use reqwest::StatusCode;
    use test_temp_dir::test_temp_dir;
    use tower::ServiceExt;
    use trow::configuration::{ConfigFile, RegistryProxiesConfig, SingleRegistryProxyConfig};

    use crate::common::trow_router;

    struct TrowInstance {
        pid: Child,
        port: u16,
    }

    /// Call out to cargo to start trow.
    async fn start_trow(temp_dir: &Path, ipv6: bool) -> TrowInstance {
        let localhost = if ipv6 { "[::1]" } else { "127.0.0.1" };
        let listener = TcpListener::bind(format!("{localhost}:0")).unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let mut child = Command::new("./target/debug/trow")
            .arg(format!(
                "--bind={ip}:{port}",
                ip = if ipv6 { "[::]" } else { "0.0.0.0" }
            ))
            .arg(format!("--data-dir={}", temp_dir.display()))
            .spawn()
            .expect("failed to start");

        let uri = format!("http://{localhost}:{port}");
        let mut timeout = 50;
        let client = reqwest::Client::new();
        let mut response = client.get(&uri).send().await;
        while timeout > 0 && (response.is_err() || (response.unwrap().status() != StatusCode::OK)) {
            thread::sleep(Duration::from_millis(500));
            response = client.get(&uri).send().await;
            timeout -= 1;
        }
        if timeout == 0 {
            child.kill().unwrap();
            panic!(
                "Failed to start Trow:\n{:?}\n---\n{:?}",
                child.stdout.unwrap(),
                child.stderr.unwrap()
            );
        }
        TrowInstance { pid: child, port }
    }

    impl Drop for TrowInstance {
        fn drop(&mut self) {
            unsafe {
                libc::kill(self.pid.id() as i32, libc::SIGTERM);
            }
        }
    }

    /**
     * Run a simple push/pull against the registry.
     *
     * This assumes the container runtime is installed (podman by default, see `runtime()`).
     */
    #[tokio::test]
    #[tracing_test::traced_test]
    async fn registry_smoke_test() {
        let temp_dir = test_temp_dir!();

        //Had issues with stopping and starting trow causing test fails.
        //It might be possible to improve things with a thread_local
        let trow = start_trow(temp_dir.as_path_untracked(), false).await;

        let remote_image = "public.ecr.aws/docker/library/alpine:latest";
        let local_image = format!("127.0.0.1:{}/alpine:trow", trow.port);

        let runtime = runtime();

        println!("Running {runtime} pull alpine:latest");
        let mut status = Command::new(&runtime)
            .args(["pull", remote_image])
            .status()
            .expect("Failed to call container runtime pull - prereq for smoke test");
        assert!(status.success());

        println!("Running {runtime} tag {remote_image} {local_image}");
        status = Command::new(&runtime)
            .args(["tag", remote_image, &local_image])
            .status()
            .expect("Failed to call container runtime");
        assert!(status.success());

        println!("Running {runtime} push {local_image}");
        status = Command::new(&runtime)
            .args(["push", &local_image])
            .args(tls_verify_args())
            .status()
            .expect("Failed to call container runtime");
        assert!(status.success());

        println!("Running {runtime} rmi {local_image}");
        status = Command::new(&runtime)
            .args(["rmi", &local_image])
            .status()
            .expect("Failed to call container runtime");
        assert!(status.success());

        println!("Running {runtime} pull {local_image}");
        status = Command::new(&runtime)
            .args(["pull", &local_image])
            .args(tls_verify_args())
            .status()
            .expect("Failed to call container runtime");

        assert!(status.success());
    }

    fn new_command(cmd: &str) -> Command {
        println!("Running: {cmd}");
        let mut cmd_it = cmd.split(' ').filter(|arg| !arg.is_empty());
        let mut cmd = Command::new(cmd_it.next().unwrap());
        cmd.args(cmd_it);
        cmd
    }

    /// Container runtime driving the smoke tests, overridable with `TROW_TEST_RUNTIME=docker`.
    fn runtime() -> String {
        std::env::var("TROW_TEST_RUNTIME").unwrap_or_else(|_| "podman".to_string())
    }

    /// `--tls-verify=false` is podman-only. Docker has no equivalent flag and instead treats
    /// loopback registries as insecure by default, so it needs no argument here.
    fn tls_verify_args() -> &'static [&'static str] {
        if runtime() == "docker" {
            &[]
        } else {
            &["--tls-verify=false"]
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn pulls_from_trow_ipv4() {
        let temp_dir = test_temp_dir!();
        let data_trow0 = temp_dir.subdir_untracked("0");
        let data_trow1 = temp_dir.subdir_untracked("1");
        std::fs::create_dir(&data_trow0).unwrap();
        std::fs::create_dir(&data_trow1).unwrap();

        let trow0 = start_trow(&data_trow0, false).await;
        let trow0_host = format!("127.0.0.1:{}", trow0.port);
        let trow1 = trow_router(&data_trow1, |cfg| {
            cfg.config_file = ConfigFile {
                registry_proxies: RegistryProxiesConfig {
                    registries: vec![SingleRegistryProxyConfig {
                        host: trow0_host.clone(),
                        insecure: true,
                        ..Default::default()
                    }]
                    .into(),
                    ..Default::default()
                },
                ..Default::default()
            };
        })
        .await;

        let remote_image = "public.ecr.aws/docker/library/alpine:latest";
        let runtime = runtime();
        let tls_verify = tls_verify_args().join(" ");
        println!("Running {runtime} pull alpine:latest");
        new_command(&format!("{runtime} pull {remote_image}"))
            .status()
            .expect("Failed to call container runtime pull alpine:latest - prereq for test");
        new_command(&format!(
            "{runtime} tag {remote_image} 127.0.0.1:{}/alpine:latest",
            trow0.port
        ))
        .status()
        .expect("Failed to call container runtime tag - prereq for test");
        new_command(&format!(
            "{runtime} push 127.0.0.1:{}/alpine:latest {tls_verify}",
            trow0.port
        ))
        .status()
        .expect("Failed to call container runtime push - prereq for test");
        let resp = trow1
            .1
            .clone()
            .oneshot(
                Request::get(format!("/v2/f/{trow0_host}/alpine/manifests/latest"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    async fn pulls_from_trow_ipv6() {
        let temp_dir = test_temp_dir!();
        let data_trow0 = temp_dir.subdir_untracked("0");
        let data_trow1 = temp_dir.subdir_untracked("1");
        std::fs::create_dir(&data_trow0).unwrap();
        std::fs::create_dir(&data_trow1).unwrap();

        let trow0 = start_trow(&data_trow0, true).await;
        let trow0_host = format!("[::1]:{}", trow0.port);
        let trow1 = trow_router(&data_trow1, |cfg| {
            cfg.config_file = ConfigFile {
                registry_proxies: RegistryProxiesConfig {
                    registries: vec![SingleRegistryProxyConfig {
                        host: trow0_host.clone(),
                        insecure: true,
                        ..Default::default()
                    }]
                    .into(),
                    ..Default::default()
                },
                ..Default::default()
            };
        })
        .await;

        let remote_image = "public.ecr.aws/docker/library/alpine:latest";
        let runtime = runtime();
        let tls_verify = tls_verify_args().join(" ");
        println!("Running {runtime} pull alpine:latest");
        new_command(&format!("{runtime} pull {remote_image}"))
            .status()
            .unwrap();
        new_command(&format!(
            "{runtime} tag {remote_image} ipv6-localhost:{}/alpine:latest",
            trow0.port
        ))
        .status()
        .unwrap();
        new_command(&format!(
            "{runtime} push ipv6-localhost:{}/alpine:latest {tls_verify}",
            trow0.port
        ))
        .status()
        .unwrap();
        let resp = trow1
            .1
            .clone()
            .oneshot(
                Request::get(format!("/v2/f/{trow0_host}/alpine/manifests/latest"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
