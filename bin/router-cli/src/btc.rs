//! Bitcoin source-chain deposit: build, sign (P2WPKH), and broadcast a deposit
//! transaction through an Esplora HTTP endpoint.

use std::collections::HashMap;
use std::str::FromStr;

use bitcoin::absolute::LockTime;
use bitcoin::consensus::encode;
use bitcoin::ecdsa;
use bitcoin::hashes::Hash;
use bitcoin::secp256k1::{All, Message, Secp256k1};
use bitcoin::sighash::{EcdsaSighashType, SighashCache};
use bitcoin::transaction::Version;
use bitcoin::{
    Address, Amount, CompressedPublicKey, Network, NetworkKind, OutPoint, PrivateKey, ScriptBuf,
    Sequence, Transaction, TxIn, TxOut, Txid, Witness,
};
use eyre::{eyre, Result, WrapErr};
use serde::Deserialize;

const NETWORK: Network = Network::Bitcoin;
const DUST_SATS: u64 = 546;
// Approximate P2WPKH virtual sizes (vbytes) for fee estimation.
const TX_OVERHEAD_VB: u64 = 11;
const INPUT_VB: u64 = 68;
const OUTPUT_VB: u64 = 31;
const DEFAULT_FEERATE_SAT_VB: u64 = 4;
const FEE_TARGET_BLOCKS: &str = "6";

#[derive(Debug, Deserialize)]
struct EsploraUtxoStatus {
    confirmed: bool,
}

#[derive(Debug, Deserialize)]
struct EsploraUtxo {
    txid: String,
    vout: u32,
    value: u64,
    status: EsploraUtxoStatus,
}

struct BtcKey {
    private_key: PrivateKey,
    public_key: CompressedPublicKey,
    address: Address,
}

fn parse_key(secp: &Secp256k1<All>, private_key: &str) -> Result<BtcKey> {
    let raw = private_key.trim();
    // Accept WIF, or a raw 32-byte hex key (assumed mainnet, compressed).
    let private_key = match PrivateKey::from_wif(raw) {
        Ok(key) => key,
        Err(_) => {
            let bytes = hex::decode(raw.strip_prefix("0x").unwrap_or(raw))
                .map_err(|_| eyre!("Bitcoin private key is neither WIF nor hex"))?;
            let secret = bitcoin::secp256k1::SecretKey::from_slice(&bytes)
                .map_err(|err| eyre!("invalid Bitcoin private key bytes: {err}"))?;
            PrivateKey::new(secret, NETWORK)
        }
    };
    if private_key.network != NetworkKind::Main {
        return Err(eyre!("Bitcoin private key is not a mainnet key"));
    }
    let public_key = CompressedPublicKey::from_private_key(secp, &private_key)
        .map_err(|_| eyre!("Bitcoin private key must be compressed for P2WPKH"))?;
    let address = Address::p2wpkh(&public_key, NETWORK);
    Ok(BtcKey {
        private_key,
        public_key,
        address,
    })
}

/// The mainnet P2WPKH address a Bitcoin private key controls.
pub fn address(private_key: &str) -> Result<String> {
    let secp = Secp256k1::new();
    Ok(parse_key(&secp, private_key)?.address.to_string())
}

/// Build, sign, and broadcast the BTC deposit; returns the broadcast txid.
pub async fn send_deposit(
    esplora_url: &str,
    private_key: &str,
    order_address: &str,
    amount_sats: u64,
) -> Result<String> {
    let secp = Secp256k1::new();
    let key = parse_key(&secp, private_key)?;
    let esplora = esplora_url.trim_end_matches('/');

    let order = Address::from_str(order_address)
        .wrap_err("order deposit address is not a valid Bitcoin address")?
        .require_network(NETWORK)
        .wrap_err("order deposit address is not a Bitcoin mainnet address")?;

    let http = reqwest::Client::new();

    let utxos: Vec<EsploraUtxo> = http
        .get(format!("{esplora}/address/{}/utxo", key.address))
        .send()
        .await
        .wrap_err("esplora UTXO request failed")?
        .error_for_status()
        .wrap_err("esplora UTXO request failed")?
        .json()
        .await
        .wrap_err("could not decode the esplora UTXO response")?;
    let mut spendable: Vec<EsploraUtxo> =
        utxos.into_iter().filter(|u| u.status.confirmed).collect();
    spendable.sort_by(|a, b| b.value.cmp(&a.value));

    let feerate = fee_rate(&http, esplora).await;
    let our_spk = key.address.script_pubkey();
    let order_spk = order.script_pubkey();

    // Largest-first selection covering amount + fee (fee grows with input count).
    let mut selected: Vec<EsploraUtxo> = Vec::new();
    let mut input_total: u64 = 0;
    let mut fee: u64 = 0;
    for utxo in spendable {
        input_total += utxo.value;
        selected.push(utxo);
        let vbytes = TX_OVERHEAD_VB + INPUT_VB * selected.len() as u64 + OUTPUT_VB * 2;
        fee = vbytes * feerate;
        if input_total >= amount_sats + fee {
            break;
        }
    }
    if input_total < amount_sats + fee {
        return Err(eyre!(
            "insufficient confirmed BTC balance: have {input_total} sats, \
             need {} sats (deposit {amount_sats} + fee {fee})",
            amount_sats + fee
        ));
    }

    let change = input_total - amount_sats - fee;
    let mut output = vec![TxOut {
        value: Amount::from_sat(amount_sats),
        script_pubkey: order_spk,
    }];
    if change >= DUST_SATS {
        output.push(TxOut {
            value: Amount::from_sat(change),
            script_pubkey: our_spk.clone(),
        });
    }

    let input = selected
        .iter()
        .map(|utxo| {
            Ok(TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_str(&utxo.txid)
                        .wrap_err("esplora returned an invalid txid")?,
                    vout: utxo.vout,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            })
        })
        .collect::<Result<Vec<TxIn>>>()?;

    let mut tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input,
        output,
    };

    // Every input spends one of our own P2WPKH UTXOs.
    let mut cache = SighashCache::new(&tx);
    let mut witnesses: Vec<Witness> = Vec::with_capacity(selected.len());
    for (index, utxo) in selected.iter().enumerate() {
        let sighash = cache
            .p2wpkh_signature_hash(
                index,
                &our_spk,
                Amount::from_sat(utxo.value),
                EcdsaSighashType::All,
            )
            .wrap_err("failed to compute the P2WPKH sighash")?;
        let message = Message::from_digest(sighash.to_byte_array());
        let signature = secp.sign_ecdsa(&message, &key.private_key.inner);
        let ecdsa_signature = ecdsa::Signature {
            signature,
            sighash_type: EcdsaSighashType::All,
        };
        witnesses.push(Witness::p2wpkh(&ecdsa_signature, &key.public_key.0));
    }
    for (index, witness) in witnesses.into_iter().enumerate() {
        tx.input[index].witness = witness;
    }

    let raw = encode::serialize_hex(&tx);
    let response = http
        .post(format!("{esplora}/tx"))
        .body(raw)
        .send()
        .await
        .wrap_err("esplora broadcast request failed")?;
    let status = response.status();
    let body = response
        .text()
        .await
        .wrap_err("could not read the esplora broadcast response")?;
    if !status.is_success() {
        return Err(eyre!("esplora rejected the deposit transaction: {body}"));
    }
    Ok(body.trim().to_string())
}

/// Esplora `sat/vB` fee rate for `FEE_TARGET_BLOCKS`, falling back to a default.
async fn fee_rate(http: &reqwest::Client, esplora: &str) -> u64 {
    let estimates: Option<HashMap<String, f64>> = async {
        http.get(format!("{esplora}/fee-estimates"))
            .send()
            .await
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .await
            .ok()
    }
    .await;

    estimates
        .and_then(|map| map.get(FEE_TARGET_BLOCKS).copied())
        .map(|rate| (rate.ceil() as u64).max(1))
        .unwrap_or(DEFAULT_FEERATE_SAT_VB)
}
