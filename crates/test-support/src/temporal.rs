use std::time::Duration;

use testcontainers::{
    core::{Healthcheck, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

const TEMPORAL_IMAGE: &str = "temporalio/admin-tools";
const TEMPORAL_IMAGE_TAG: &str = "1.31.0";
const TEMPORAL_PORT: u16 = 7233;
const ROUTER_TEST_TEMPORAL_ADDRESS_ENV: &str = "ROUTER_TEST_TEMPORAL_ADDRESS";

pub struct TestTemporal {
    temporal_address: String,
    container: Option<ContainerAsync<GenericImage>>,
}

impl TestTemporal {
    #[must_use]
    pub fn temporal_address(&self) -> &str {
        &self.temporal_address
    }

    pub async fn shutdown(&mut self) {
        if let Some(container) = self.container.take() {
            container.rm().await.expect("remove Temporal testcontainer");
        }
    }

    pub async fn start() -> Self {
        if let Ok(temporal_address) = std::env::var(ROUTER_TEST_TEMPORAL_ADDRESS_ENV) {
            return Self {
                temporal_address,
                container: None,
            };
        }

        let image = GenericImage::new(TEMPORAL_IMAGE, TEMPORAL_IMAGE_TAG)
            .with_exposed_port(TEMPORAL_PORT.tcp())
            .with_wait_for(WaitFor::healthcheck())
            .with_health_check(
                Healthcheck::cmd([
                    "temporal",
                    "operator",
                    "cluster",
                    "health",
                    "--address",
                    "127.0.0.1:7233",
                ])
                .with_interval(Duration::from_secs(1))
                .with_timeout(Duration::from_secs(2))
                .with_start_period(Duration::from_secs(5))
                .with_retries(90),
            )
            .with_cmd([
                "temporal".to_string(),
                "server".to_string(),
                "start-dev".to_string(),
                "--ip".to_string(),
                "0.0.0.0".to_string(),
                "--port".to_string(),
                TEMPORAL_PORT.to_string(),
                "--headless".to_string(),
            ]);

        let container = image.start().await.unwrap_or_else(|err| {
            panic!(
                "failed to start Temporal testcontainer {TEMPORAL_IMAGE}:{TEMPORAL_IMAGE_TAG}: {err}"
            )
        });
        let port = container
            .get_host_port_ipv4(TEMPORAL_PORT.tcp())
            .await
            .expect("read Temporal testcontainer port");
        let temporal_address = format!("http://127.0.0.1:{port}");

        Self {
            temporal_address,
            container: Some(container),
        }
    }
}

impl Drop for TestTemporal {
    fn drop(&mut self) {
        let Some(container) = self.container.take() else {
            return;
        };

        let cleanup = match std::thread::Builder::new()
            .name("temporal-testcontainer-cleanup".to_string())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    eprintln!("failed to build Temporal testcontainer cleanup runtime");
                    return;
                };
                if let Err(err) = runtime.block_on(container.rm()) {
                    eprintln!("failed to remove Temporal testcontainer during drop: {err}");
                }
            }) {
            Ok(cleanup) => cleanup,
            Err(err) => {
                eprintln!("failed to spawn Temporal testcontainer cleanup thread: {err}");
                return;
            }
        };
        let _ = cleanup.join();
    }
}
