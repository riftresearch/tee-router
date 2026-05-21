//! `router-cli swap` — quote, confirm, create the order, broadcast the deposit.

use std::io::{self, Write};

use eyre::{eyre, Result, WrapErr};
use router_gateway_sdk::{
    AmountFormat, CreateOrderRequest, GatewayClient, QuoteRequest, RefundMode,
};
use uuid::Uuid;

use crate::assets::{self, SourceChain};
use crate::{btc, evm};

/// Inputs for a swap run.
pub struct SwapArgs {
    pub gateway_url: String,
    pub rpc_url: String,
    pub private_key: String,
    pub from: String,
    pub to: String,
    pub from_amount: String,
    pub to_address: String,
    pub auto_accept: bool,
}

pub async fn run(args: SwapArgs) -> Result<()> {
    let source = assets::resolve_source(&args.from)?;
    let client =
        GatewayClient::new(args.gateway_url.clone()).wrap_err("invalid --gateway-url")?;

    // The deposit is signed on the source chain, so the sender address is
    // derived from the private key per the source asset's chain family.
    let from_address = match source.chain {
        SourceChain::Evm { .. } => evm::address(&args.private_key)?,
        SourceChain::Bitcoin => btc::address(&args.private_key)?,
    };

    // 1. Quote.
    let quote = client
        .quote(&QuoteRequest {
            from: source.identifier.to_string(),
            to: args.to.clone(),
            amount_format: Some(AmountFormat::Readable),
            to_address: args.to_address.clone(),
            from_amount: args.from_amount.clone(),
        })
        .await
        .wrap_err("quote request failed")?;

    println!("Quote");
    println!("  send:          {} {}", args.from_amount, quote.from);
    println!("  estimated out: {} {}", quote.estimated_out, quote.to);
    if let Some(fees) = &quote.fees {
        for fee in fees {
            println!("  fee:           {} {} ({})", fee.amount, fee.asset, fee.label);
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

    // 3. Create the order. Token-mode refunds are chain-agnostic, so this path
    //    works whether the source chain is EVM or Bitcoin.
    let order = client
        .create_market_order(&CreateOrderRequest {
            quote_id: quote.quote_id.clone(),
            from_address: from_address.clone(),
            to_address: args.to_address.clone(),
            refund_address: Some(from_address.clone()),
            idempotency_key: format!("router-cli-{}", Uuid::now_v7()),
            refund_mode: Some(RefundMode::Token),
            refund_authorizer: None,
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
            let sats = order.amount_to_send.parse::<u64>().map_err(|err| {
                eyre!("invalid raw BTC amount `{}`: {err}", order.amount_to_send)
            })?;
            btc::send_deposit(&args.rpc_url, &args.private_key, &order.order_address, sats)
                .await?
        }
    };

    // 5. Report and exit.
    println!();
    println!("Order created.");
    println!("  order id:     {}", order.order_id);
    println!("  deposit tx:   {txid}");
    println!("  status:       {}", order.status);
    if let Some(token) = &order.refund_token {
        println!("  refund token: {token}");
    }
    println!();
    println!(
        "Track it:  router-cli status {} --gateway-url {}",
        order.order_id, args.gateway_url
    );
    Ok(())
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
