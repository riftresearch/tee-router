use alloy::signers::local::PrivateKeySigner;
use bitcoin::Network;
use blockchain_utils::P2WPKHBitcoinWallet;
use clap::{Parser, ValueEnum};
use rand::{rngs::StdRng, Rng, SeedableRng};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum NetworkArg {
    Regtest,
    Mainnet,
}

impl From<NetworkArg> for Network {
    fn from(arg: NetworkArg) -> Self {
        match arg {
            NetworkArg::Regtest => Network::Regtest,
            NetworkArg::Mainnet => Network::Bitcoin,
        }
    }
}

#[derive(Parser)]
#[command(name = "rand-wallet")]
#[command(about = "Generate random Bitcoin and EVM wallets")]
struct Cli {
    /// Bitcoin network to use
    #[arg(long, value_enum)]
    network: NetworkArg,
}

fn main() {
    let cli = Cli::parse();
    let network: Network = cli.network.into();

    // Bitcoin wallet
    let btc_wallet = loop {
        let btc_rand_bytes: [u8; 32] = StdRng::from_os_rng().random();
        if let Ok(wallet) = P2WPKHBitcoinWallet::from_secret_bytes(&btc_rand_bytes, network) {
            break wallet;
        }
    };

    println!("=== Bitcoin Wallet ===");
    println!("wif: {}", btc_wallet.private_key);
    println!("descriptor: {}", btc_wallet.descriptor());
    println!("address: {}", btc_wallet.address);

    // EVM wallet
    let (evm_rand_bytes, evm_signer) = loop {
        let evm_rand_bytes: [u8; 32] = StdRng::from_os_rng().random();
        if let Ok(signer) = PrivateKeySigner::from_bytes(&evm_rand_bytes.into()) {
            break (evm_rand_bytes, signer);
        }
    };

    println!();
    println!("=== EVM Wallet ===");
    println!("private_key: 0x{}", hex::encode(evm_rand_bytes));
    println!("address: {}", evm_signer.address());
}
