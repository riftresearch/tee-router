use std::{error::Error, net::SocketAddr, time::Duration};

use alloy::{
    hex,
    network::{EthereumWallet, TransactionBuilder},
    node_bindings::Anvil,
    primitives::U256,
    providers::{ext::AnvilApi, Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use evm_receipt_watcher::{build_router, AppState, Config, PendingWatches, ReceiptPubSub, Watcher};
use evm_receipt_watcher_client::{EvmReceiptWatcherClient, WatchStatus};
use tokio::{net::TcpListener, time::timeout};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn confirms_registered_tx_hash_against_anvil() -> TestResult<()> {
    let anvil = Anvil::new().arg("--no-mining").try_spawn()?;
    let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
    let signer = format!("0x{}", hex::encode(private_key)).parse::<PrivateKeySigner>()?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(anvil.endpoint_url());

    let bind = "127.0.0.1:0".parse::<SocketAddr>()?;
    let config = Config {
        chain: "anvil".to_string(),
        http_rpc_url: anvil.endpoint_url().to_string(),
        ws_rpc_url: Some(anvil.ws_endpoint()),
        bind,
        max_pending: 100,
        max_subscriber_lag: 100,
        poll_interval_ms: 100,
        ws_reconnect_delay_ms: 100,
        receipt_retry_count: 3,
        receipt_retry_delay_ms: 50,
    };

    let pending = PendingWatches::new(config.chain.clone(), config.max_pending);
    let pubsub = ReceiptPubSub::new(config.max_subscriber_lag);
    let watcher = Watcher::new(&config, pending.clone(), pubsub.clone()).await?;
    let receipt_provider = watcher.receipt_provider();
    tokio::spawn(async move {
        watcher.run().await;
    });

    let app = build_router(AppState {
        chain: config.chain.clone(),
        pending,
        pubsub,
        receipt_provider,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(?error, "test EVM receipt watcher server stopped");
        }
    });

    let base_url = format!("http://{addr}");
    wait_for_health(&base_url).await?;
    let client = EvmReceiptWatcherClient::new(&base_url, "anvil")?;
    let mut events = client.subscribe().await?;

    let tx = TransactionRequest::default()
        .with_to(anvil.addresses()[1])
        .with_value(U256::from(1_u64));
    let pending_tx = provider.send_transaction(tx).await?;
    let tx_hash = *pending_tx.tx_hash();

    client.watch(tx_hash, "operation-1").await?;
    provider.anvil_mine(Some(1), None).await?;

    let confirmed = timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            let event = event?;
            if event.tx_hash == tx_hash && event.status == WatchStatus::Confirmed {
                return Ok::<_, evm_receipt_watcher_client::Error>(event);
            }
        }
        Err(evm_receipt_watcher_client::Error::WebSocket {
            source: "watch stream closed before confirmation".to_string(),
        })
    })
    .await??;

    assert_eq!(confirmed.requesting_operation_id, "operation-1");
    assert!(confirmed.receipt.is_some());
    Ok(())
}

async fn wait_for_health(base_url: &str) -> TestResult<()> {
    let http = reqwest::Client::new();
    for _ in 0..50 {
        match http.get(format!("{base_url}/healthz")).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "EVM receipt watcher test server did not become healthy",
    )
    .into())
}
