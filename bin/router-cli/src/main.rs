//! `router-cli` — a command-line client/SDK shim for the Rift router gateway.
//!
//! `swap` runs a full source-to-destination swap: quote, confirm, create the
//! order, and broadcast the source-chain deposit. `status` reads (or watches)
//! an existing order.

mod assets;
mod btc;
mod evm;
mod status;
mod swap;

use clap::{Args, Parser, Subcommand};
use eyre::Result;

#[derive(Parser)]
#[command(
    name = "router-cli",
    about = "Command-line client for the Rift router gateway",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Quote, confirm, create an order, and broadcast the source-chain deposit.
    Swap(SwapCmd),
    /// Read (or watch) the status of an existing order.
    Status(StatusCmd),
}

#[derive(Args)]
struct SwapCmd {
    /// Public router gateway base URL.
    #[arg(long)]
    gateway_url: String,
    /// Source-chain RPC: an EVM JSON-RPC URL, or a Bitcoin Esplora base URL.
    #[arg(long)]
    rpc_url: String,
    /// Source-chain private key (EVM hex, or Bitcoin WIF/hex).
    #[arg(long, env = "ROUTER_CLI_PRIVATE_KEY", hide_env_values = true)]
    private_key: String,
    /// Source asset, e.g. `Ethereum.USDC` or `Bitcoin.BTC`.
    #[arg(long)]
    from: String,
    /// Destination asset, e.g. `Base.USDC`.
    #[arg(long)]
    to: String,
    /// Source amount in readable units, e.g. `100`.
    #[arg(long)]
    from_amount: String,
    /// Recipient address on the destination chain.
    #[arg(long)]
    to_address: String,
    /// Accept the quote without an interactive confirmation prompt.
    #[arg(long, short = 'y')]
    yes: bool,
}

#[derive(Args)]
struct StatusCmd {
    /// Order id returned by `swap`.
    order_id: String,
    /// Public router gateway base URL.
    #[arg(long)]
    gateway_url: String,
    /// Poll until the order reaches a terminal status.
    #[arg(long)]
    watch: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Swap(cmd) => {
            swap::run(swap::SwapArgs {
                gateway_url: cmd.gateway_url,
                rpc_url: cmd.rpc_url,
                private_key: cmd.private_key,
                from: cmd.from,
                to: cmd.to,
                from_amount: cmd.from_amount,
                to_address: cmd.to_address,
                auto_accept: cmd.yes,
            })
            .await
        }
        Command::Status(cmd) => {
            status::run(status::StatusArgs {
                gateway_url: cmd.gateway_url,
                order_id: cmd.order_id,
                watch: cmd.watch,
            })
            .await
        }
    }
}
