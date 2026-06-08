//! `router-gateway-cli swap` — quote, confirm, create the order, broadcast the deposit.

use std::io::{self, Write};

use eyre::{eyre, Result, WrapErr};
use router_gateway_sdk::{
    AmountFormat, CreateOrderRequest, GatewayClient, ProviderId, QuoteRequest, QuoteRouting,
};
use uuid::Uuid;

use crate::assets::{self, SourceChain};
use crate::{btc, evm, hyperliquid};

/// Inputs for a swap run.
pub struct SwapArgs {
    pub gateway_url: String,
    pub rpc_url: String,
    pub private_key: String,
    pub from: String,
    pub to: String,
    pub from_amount: String,
    pub to_address: String,
    pub provider_sequence: Option<String>,
    pub auto_accept: bool,
}

pub async fn run(args: SwapArgs) -> Result<()> {
    let source = assets::resolve_source(&args.from)?;
    let client = GatewayClient::new(args.gateway_url.clone()).wrap_err("invalid --gateway-url")?;

    // The deposit is signed on the source chain, so the sender address is
    // derived from the private key per the source asset's chain family.
    let from_address = match source.chain {
        SourceChain::Evm { .. } => evm::address(&args.private_key)?,
        SourceChain::Bitcoin => btc::address(&args.private_key)?,
        SourceChain::HyperliquidSpot { .. } => hyperliquid::address(&args.private_key)?,
    };

    let routing = args
        .provider_sequence
        .as_deref()
        .map(parse_provider_sequence)
        .transpose()?;
    // 1. Quote.
    let quote = client
        .quote(&QuoteRequest {
            from: source.identifier.to_string(),
            to: args.to.clone(),
            amount_format: Some(AmountFormat::Readable),
            from_amount: args.from_amount.clone(),
            routing,
        })
        .await
        .wrap_err("quote request failed")?;

    println!("Quote");
    println!("  send:          {} {}", args.from_amount, quote.from);
    println!("  estimated out: {} {}", quote.estimated_out, quote.to);
    if let Some(fees) = &quote.fees {
        for fee in fees {
            println!(
                "  fee:           {} {} ({})",
                fee.amount, fee.asset, fee.label
            );
        }
    }
    println!("  quote id:      {}", quote.quote_id);
    println!("  expires:       {}", quote.expiry);
    println!();

    // 2. Confirm (unless auto-accepted).
    if !args.auto_accept && !confirm("Accept this quote and broadcast the deposit?")? {
        println!("Aborted — no order created.");
        return Ok(());
    }

    // 3. Create the order.
    let order = client
        .create_market_order(&CreateOrderRequest {
            quote_id: quote.quote_id.clone(),
            from_address: from_address.clone(),
            to_address: args.to_address.clone(),
            refund_address: Some(from_address.clone()),
            idempotency_key: format!("router-gateway-cli-{}", Uuid::now_v7()),
            amount_format: Some(AmountFormat::Raw),
        })
        .await
        .wrap_err("order creation failed")?;

    // 4. Sign and broadcast the deposit on the source chain.
    println!(
        "Broadcasting deposit of {} (raw units) to {} ...",
        order.amount_to_send, order.order_address
    );
    let txid = match source.chain {
        SourceChain::Evm { chain_id } => {
            evm::send_deposit(
                &args.rpc_url,
                &args.private_key,
                source.evm_token,
                chain_id,
                &order.order_address,
                &order.amount_to_send,
            )
            .await?
        }
        SourceChain::Bitcoin => {
            let sats = order
                .amount_to_send
                .parse::<u64>()
                .map_err(|err| eyre!("invalid raw BTC amount `{}`: {err}", order.amount_to_send))?;
            btc::send_deposit(&args.rpc_url, &args.private_key, &order.order_address, sats).await?
        }
        SourceChain::HyperliquidSpot {
            token_symbol,
            decimals,
        } => {
            hyperliquid::send_deposit(
                &args.private_key,
                &order.order_address,
                token_symbol,
                decimals,
                &order.amount_to_send,
            )
            .await?
        }
    };

    // 5. Report and exit.
    println!();
    println!("Order created.");
    println!("  order id:     {}", order.order_id);
    println!("  deposit tx:   {txid}");
    println!("  status:       {}", order.status);
    println!();
    println!(
        "Track it:  router-gateway-cli status {} --gateway-url {}",
        order.order_id, args.gateway_url
    );
    Ok(())
}
fn parse_provider_sequence(value: &str) -> Result<QuoteRouting> {
    let mut providers = Vec::new();
    for raw_provider in value.split(',') {
        let provider = raw_provider.trim();
        if provider.is_empty() {
            return Err(eyre!("--provider-sequence contains an empty provider"));
        }
        providers.push(parse_provider_id(provider)?);
    }
    if providers.is_empty() {
        return Err(eyre!(
            "--provider-sequence must contain at least one provider"
        ));
    }
    Ok(QuoteRouting {
        provider_sequence: Some(providers),
    })
}

fn parse_provider_id(value: &str) -> Result<ProviderId> {
    ProviderId::parse(value).ok_or_else(|| {
        eyre!(
            "unknown provider `{value}`; expected one of: across, cctp, unit, hyperliquid_bridge, hyperliquid_spot, velora, relay, near_intents, mayan, chainflip, garden"
        )
    })
}

fn confirm(prompt: &str) -> Result<bool> {
    print!("{prompt} [y/N] ");
    io::stdout().flush().ok();
    let mut answer = String::new();
    io::stdin()
        .read_line(&mut answer)
        .wrap_err("failed to read confirmation from stdin")?;
    Ok(matches!(
        answer.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}
