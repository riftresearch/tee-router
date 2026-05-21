//! `router-cli status` — read, or watch, an order's status.

use std::time::Duration;

use eyre::{Result, WrapErr};
use router_gateway_sdk::{GatewayClient, OrderResponse};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Inputs for a status run.
pub struct StatusArgs {
    pub gateway_url: String,
    pub order_id: String,
    pub watch: bool,
}

pub async fn run(args: StatusArgs) -> Result<()> {
    let client =
        GatewayClient::new(args.gateway_url).wrap_err("invalid --gateway-url")?;
    loop {
        let order = client
            .get_order(&args.order_id)
            .await
            .wrap_err("failed to read order status")?;
        print_order(&order);
        if !args.watch || is_terminal(&order.status) {
            return Ok(());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
        println!();
    }
}

fn print_order(order: &OrderResponse) {
    println!("Order {}", order.order_id);
    println!("  status:        {}", order.status);
    println!("  route:         {} -> {}", order.from, order.to);
    println!("  deposit addr:  {}", order.order_address);
    println!("  amount to send:{}", order.amount_to_send);
    println!("  estimated out: {}", order.estimated_out);
    println!("  quote id:      {}", order.quote_id);
    println!("  expires:       {}", order.expiry);
}

/// Whether a status string looks terminal — used to stop `--watch` polling.
fn is_terminal(status: &str) -> bool {
    let status = status.to_ascii_lowercase();
    ["settl", "complet", "cancel", "fail", "refund", "expir"]
        .iter()
        .any(|terminal| status.contains(terminal))
}
