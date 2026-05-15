use std::{error::Error, net::SocketAddr, time::Duration};

use bitcoin::{Amount, BlockHash, Network};
use bitcoin_indexer::{build_router, AppState, BitcoinIndexer, Config, IndexerPubSub};
use bitcoin_indexer_client::{BitcoinIndexerClient, SubscribeFilter, TxOutputQuery};
use bitcoincore_rpc_async::{Client as BitcoinRpcClient, RpcApi};
use devnet::RiftDevnet;
use serde::Deserialize;
use tokio::{net::TcpListener, time::timeout};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn streams_and_queries_regtest_outputs() -> TestResult<()> {
    let (devnet, _) = RiftDevnet::builder().using_esplora(true).build().await?;
    let bind = "127.0.0.1:0".parse::<SocketAddr>()?;
    let config = Config {
        network: Network::Regtest,
        rpc_url: devnet.bitcoin.rpc_url.clone(),
        rpc_auth: Some(format!("cookie:{}", devnet.bitcoin.cookie.display())),
        esplora_url: devnet
            .bitcoin
            .esplora_url
            .clone()
            .ok_or("devnet esplora URL missing")?,
        zmq_rawblock_endpoint: devnet.bitcoin.zmq_rawblock_endpoint.clone(),
        zmq_rawtx_endpoint: devnet.bitcoin.zmq_rawtx_endpoint.clone(),
        bind,
        max_subscriber_lag: 100,
        poll_interval_ms: 100,
        zmq_reconnect_delay_ms: 100,
        reorg_rescan_depth: 6,
    };

    let pubsub = IndexerPubSub::new(config.max_subscriber_lag);
    let indexer = BitcoinIndexer::new(&config, pubsub.clone()).await?;
    indexer.clone().run().await;
    let app = build_router(AppState {
        indexer,
        pubsub,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(?error, "test Bitcoin indexer server stopped");
        }
    });

    let base_url = format!("http://{addr}");
    wait_for_health(&base_url).await?;
    let client = BitcoinIndexerClient::new(&base_url)?;
    let address = devnet
        .bitcoin
        .rpc_client
        .get_new_address(None, None)
        .await?
        .require_network(Network::Regtest)?;
    let mut events = client
        .subscribe(SubscribeFilter {
            addresses: vec![address.clone().into_unchecked()],
            min_amount: Some(50_000),
        })
        .await?;

    devnet
        .bitcoin
        .rpc_client
        .send_to_address(&address, Amount::from_sat(50_000))
        .await?;

    let mempool = timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            let event = event?;
            if event.address.assume_checked_ref().to_string() == address.to_string()
                && event.block_height.is_none()
            {
                return Ok::<_, bitcoin_indexer_client::Error>(event);
            }
        }
        Err(bitcoin_indexer_client::Error::WebSocket {
            source: "output stream closed before mempool event".to_string(),
        })
    })
    .await??;
    assert_eq!(mempool.amount_sats, 50_000);

    devnet.bitcoin.mine_blocks(1).await?;
    let confirmed = timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            let event = event?;
            if event.address.assume_checked_ref().to_string() == address.to_string()
                && event.block_height.is_some()
            {
                return Ok::<_, bitcoin_indexer_client::Error>(event);
            }
        }
        Err(bitcoin_indexer_client::Error::WebSocket {
            source: "output stream closed before confirmed event".to_string(),
        })
    })
    .await??;
    assert_eq!(confirmed.confirmations, 1);

    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(10))
        .await?;
    let page = client
        .tx_outputs(TxOutputQuery {
            address: address.into_unchecked(),
            from_block: Some(0),
            min_amount: Some(50_000),
            limit: Some(10),
            cursor: None,
        })
        .await?;
    assert!(page
        .outputs
        .iter()
        .any(|output| output.txid == confirmed.txid));
    Ok(())
}

#[tokio::test]
#[ignore = "integration: spawns devnet stack"]
async fn reorg_retracts_outputs_from_invalidated_regtest_block() -> TestResult<()> {
    let (devnet, _) = RiftDevnet::builder().using_esplora(true).build().await?;
    let bind = "127.0.0.1:0".parse::<SocketAddr>()?;
    let config = Config {
        network: Network::Regtest,
        rpc_url: devnet.bitcoin.rpc_url.clone(),
        rpc_auth: Some(format!("cookie:{}", devnet.bitcoin.cookie.display())),
        esplora_url: devnet
            .bitcoin
            .esplora_url
            .clone()
            .ok_or("devnet esplora URL missing")?,
        zmq_rawblock_endpoint: devnet.bitcoin.zmq_rawblock_endpoint.clone(),
        zmq_rawtx_endpoint: devnet.bitcoin.zmq_rawtx_endpoint.clone(),
        bind,
        max_subscriber_lag: 100,
        poll_interval_ms: 100,
        zmq_reconnect_delay_ms: 100,
        reorg_rescan_depth: 6,
    };

    let pubsub = IndexerPubSub::new(config.max_subscriber_lag);
    let indexer = BitcoinIndexer::new(&config, pubsub.clone()).await?;
    indexer.clone().run().await;
    let app = build_router(AppState {
        indexer,
        pubsub,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            tracing::warn!(?error, "test Bitcoin indexer server stopped");
        }
    });

    let base_url = format!("http://{addr}");
    wait_for_health(&base_url).await?;
    let client = BitcoinIndexerClient::new(&base_url)?;
    let address = devnet
        .bitcoin
        .rpc_client
        .get_new_address(None, None)
        .await?
        .require_network(Network::Regtest)?;
    let mut events = client
        .subscribe(SubscribeFilter {
            addresses: vec![address.clone().into_unchecked()],
            min_amount: Some(50_000),
        })
        .await?;

    devnet
        .bitcoin
        .rpc_client
        .send_to_address(&address, Amount::from_sat(50_000))
        .await?;
    devnet.bitcoin.mine_blocks(1).await?;

    let confirmed = timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            let event = event?;
            if event.address.assume_checked_ref().to_string() == address.to_string()
                && event.block_height.is_some()
                && !event.removed
            {
                return Ok::<_, bitcoin_indexer_client::Error>(event);
            }
        }
        Err(bitcoin_indexer_client::Error::WebSocket {
            source: "output stream closed before confirmed event".to_string(),
        })
    })
    .await??;

    let reorged_hash = confirmed
        .block_hash
        .ok_or("confirmed event missing block hash")?;
    devnet
        .bitcoin
        .rpc_client
        .invalidate_block(&reorged_hash)
        .await?;
    mine_empty_block(
        devnet.bitcoin.rpc_client.as_ref(),
        &devnet.bitcoin.miner_address,
    )
    .await?;

    let retracted = timeout(Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            let event = event?;
            if event.txid == confirmed.txid && event.vout == confirmed.vout && event.removed {
                return Ok::<_, bitcoin_indexer_client::Error>(event);
            }
        }
        Err(bitcoin_indexer_client::Error::WebSocket {
            source: "output stream closed before retraction event".to_string(),
        })
    })
    .await??;

    assert_eq!(retracted.block_hash, confirmed.block_hash);
    assert_eq!(retracted.block_height, confirmed.block_height);
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
        "Bitcoin indexer test server did not become healthy",
    )
    .into())
}

async fn mine_empty_block(
    rpc: &BitcoinRpcClient,
    address: &bitcoin::Address,
) -> TestResult<BlockHash> {
    #[derive(Deserialize)]
    struct GenerateBlockResponse {
        hash: BlockHash,
    }

    let response: GenerateBlockResponse = rpc
        .call(
            "generateblock",
            &[
                serde_json::to_value(address.to_string())?,
                serde_json::to_value(Vec::<String>::new())?,
            ],
        )
        .await?;
    Ok(response.hash)
}
