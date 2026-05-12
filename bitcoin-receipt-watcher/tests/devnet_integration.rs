use std::{error::Error, net::SocketAddr, time::Duration};

use bitcoin::{Amount, Network};
use bitcoin_receipt_watcher::{
    build_router, AppState, Config, PendingWatches, ReceiptPubSub, Watcher,
};
use bitcoin_receipt_watcher_client::{BitcoinReceiptWatcherClient, WatchStatus};
use bitcoincore_rpc_async::RpcApi;
use devnet::RiftDevnet;
use tokio::{net::TcpListener, time::timeout};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn confirms_registered_txid_against_regtest() -> TestResult<()> {
    let (devnet, _) = RiftDevnet::builder().using_esplora(true).build().await?;
    let bind = "127.0.0.1:0".parse::<SocketAddr>()?;
    let config = Config {
        chain: "bitcoin".to_string(),
        rpc_url: devnet.bitcoin.rpc_url.clone(),
        rpc_auth: Some(format!("cookie:{}", devnet.bitcoin.cookie.display())),
        zmq_rawblock_endpoint: devnet.bitcoin.zmq_rawblock_endpoint.clone(),
        bind,
        max_pending: 100,
        max_subscriber_lag: 100,
        poll_interval_ms: 100,
        zmq_reconnect_delay_ms: 100,
    };

    let pending = PendingWatches::new(config.chain.clone(), config.max_pending);
    let pubsub = ReceiptPubSub::new(config.max_subscriber_lag);
    let watcher = Watcher::new(&config, pending.clone(), pubsub.clone()).await?;
    let rpc = watcher.rpc_client();
    watcher.clone().run().await;

    let app = build_router(AppState {
        chain: config.chain.clone(),
        pending,
        pubsub,
        rpc,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(?error, "test Bitcoin receipt watcher server stopped");
        }
    });

    let base_url = format!("http://{addr}");
    wait_for_health(&base_url).await?;
    let client = BitcoinReceiptWatcherClient::new(&base_url, "bitcoin")?;
    let mut events = client.subscribe().await?;

    let address = devnet
        .bitcoin
        .rpc_client
        .get_new_address(None, None)
        .await?
        .require_network(Network::Regtest)?;
    let txid = devnet
        .bitcoin
        .rpc_client
        .send_to_address(&address, Amount::from_sat(50_000))
        .await?
        .txid()?;

    client.watch(txid, "operation-1").await?;
    devnet.bitcoin.mine_blocks(1).await?;

    let confirmed = timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            let event = event?;
            if event.txid == txid && event.status == WatchStatus::Confirmed {
                return Ok::<_, bitcoin_receipt_watcher_client::Error>(event);
            }
        }
        Err(bitcoin_receipt_watcher_client::Error::WebSocket {
            source: "watch stream closed before confirmation".to_string(),
        })
    })
    .await??;

    assert_eq!(confirmed.requesting_operation_id, "operation-1");
    let receipt = confirmed.receipt.ok_or("missing receipt")?;
    assert_eq!(receipt.confirmations, 1);
    assert_eq!(receipt.tx.compute_txid(), txid);
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
        "Bitcoin receipt watcher test server did not become healthy",
    )
    .into())
}
