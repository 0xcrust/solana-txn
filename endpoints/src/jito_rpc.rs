use std::sync::Arc;

use anyhow::{Result, anyhow};
use base64::{Engine, prelude::BASE64_STANDARD};
use futures::StreamExt;
use log::{debug, error, info};
use rand::Rng;
use serde::{Deserialize, Serialize, de};
use serde_json::{Value, json};
use solana_program::{program_utils::limited_deserialize, pubkey};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    system_instruction::SystemInstruction,
    transaction::VersionedTransaction,
};
use solana_sdk_ids::system_program;
use solana_svm_transaction::instruction::SVMInstruction;
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use tokio::sync::RwLock;

// https://jito-labs.gitbook.io/mev/searcher-services/recommendations#bundle-assembly
// - Ensure your tip is an instruction in the last transaction.
// - Run a balance assertion after tipping to ensure the transaction doesn't result in a loss.
// - Provide slot range checks to confirm bundles land within the expected range.
// - To enhance Solana's performance, randomly choose one of the eight tip accounts to transfer SOL to, maximizing the chance of parallelism.

// https://jito-labs.gitbook.io/mev/searcher-resources/bundles

const BUNDLES: &str = "/api/v1/bundles";
const GET_BUNDLE_STATUSES: &str = "/api/v1/getBundleStatuses";
const GET_INFLIGHT_BUNDLE_STATUSES: &str = "/api/v1/getInflightBundleStatuses";
const TXNS: &str = "/api/v1/transactions";

#[derive(Debug, Deserialize)]
pub struct JitoResponse<T> {
    pub result: T,
}

#[derive(Clone)]
pub struct JitoClient {
    client: reqwest::Client,
    region: JitoRegion,
}

#[derive(Copy, Clone, Debug, Default, Hash, Serialize, Deserialize, Eq, PartialEq)]
pub enum JitoRegion {
    Amsterdam,
    Frankfurt,
    London,
    #[default]
    NewYork,
    SaltLakeCity,
    Singapore,
    Tokyo,
}

pub const JITO_REGIONS: [JitoRegion; 7] = [
    JitoRegion::Amsterdam,
    JitoRegion::Frankfurt,
    JitoRegion::London,
    JitoRegion::NewYork,
    JitoRegion::SaltLakeCity,
    JitoRegion::Singapore,
    JitoRegion::Tokyo,
];

impl JitoRegion {
    pub fn urls(&self) -> &BlockEngine<'_> {
        match self {
            JitoRegion::Amsterdam => &JITO_BLOCK_ENGINES[0],
            JitoRegion::Frankfurt => &JITO_BLOCK_ENGINES[1],
            JitoRegion::London => &JITO_BLOCK_ENGINES[2],
            JitoRegion::NewYork => &JITO_BLOCK_ENGINES[3],
            JitoRegion::SaltLakeCity => &JITO_BLOCK_ENGINES[4],
            JitoRegion::Singapore => &JITO_BLOCK_ENGINES[5],
            JitoRegion::Tokyo => &JITO_BLOCK_ENGINES[6],
        }
    }
}

impl std::str::FromStr for JitoRegion {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "amsterdam" => Ok(JitoRegion::Amsterdam),
            "frankfurt" => Ok(JitoRegion::Frankfurt),
            "london" => Ok(JitoRegion::London),
            "newyork" => Ok(JitoRegion::NewYork),
            "singapore" => Ok(JitoRegion::Singapore),
            "slc" => Ok(JitoRegion::SaltLakeCity),
            "tokyo" => Ok(JitoRegion::Tokyo),
            _ => Err(anyhow!(
                "Expected one of 'amsterdam', 'newyork', 'frankfurt', 'tokyo', 'singapore', 'london', 'slc'"
            )),
        }
    }
}

impl std::fmt::Display for JitoRegion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JitoRegion::Amsterdam => f.write_str("amsterdam"),
            JitoRegion::Frankfurt => f.write_str("frankfurt"),
            JitoRegion::London => f.write_str("london"),
            JitoRegion::NewYork => f.write_str("newyork"),
            JitoRegion::Singapore => f.write_str("singapore"),
            JitoRegion::SaltLakeCity => f.write_str("slc"),
            JitoRegion::Tokyo => f.write_str("tokyo"),
        }
    }
}

impl JitoClient {
    pub const MAX_GET_BUNDLE_STATUSES_QUERY_ITEMS: usize = 5;

    pub fn new(region: JitoRegion) -> Self {
        JitoClient {
            client: reqwest::Client::new(),
            region,
        }
    }

    pub fn build_bribe_ix(from: &Pubkey, value: u64) -> solana_sdk::instruction::Instruction {
        solana_sdk::system_instruction::transfer(from, pick_jito_recipient(), value)
    }

    pub async fn send_transaction(
        &self,
        tx: &VersionedTransaction,
        region_override: Option<JitoRegion>,
    ) -> Result<String> {
        let encoded = serialize_and_encode(tx, UiTransactionEncoding::Base64)?;
        let endpoint = format!(
            "{}{}",
            region_override.unwrap_or(self.region).urls().url,
            TXNS
        );

        Ok(self
            .make_request::<JitoResponse<String>>(
                &endpoint,
                "sendTransaction",
                json!([encoded, {"encoding": "base64"}]),
            )
            .await?
            .result)
    }

    /// Send a bundle to Jito. The signature of the first txn serves as the identifier for the entire bundle
    pub async fn send_bundle(
        &self,
        bundle: &[&VersionedTransaction],
        region_override: Option<JitoRegion>,
    ) -> Result<String> {
        let expected_length = bundle.len();
        let bundle = bundle
            .iter()
            .filter_map(|tx| serialize_and_encode(&tx, UiTransactionEncoding::Base64).ok()) // unwrap should be fine too
            .collect::<Vec<_>>();
        if bundle.len() != expected_length {
            return Err(anyhow!("Failed to serialize bundles"));
        }

        let endpoint = format!(
            "{}{}",
            region_override.unwrap_or(self.region).urls().url,
            BUNDLES
        );
        Ok(self
            .make_request::<JitoResponse<String>>(
                &endpoint,
                "sendBundle",
                json!([bundle, {"encoding": "base64"}]),
            )
            .await?
            .result)
    }

    /// Get the confirmation status for up to a maximum of 5 bundles
    pub async fn get_bundle_statuses(
        &self,
        ids: &[String],
        region_override: Option<JitoRegion>,
    ) -> Result<WithContext<Vec<BundleStatus>>> {
        let endpoint = format!(
            "{}{}",
            region_override.unwrap_or(self.region).urls().url,
            GET_BUNDLE_STATUSES
        );

        Ok(self
            .make_request::<JitoResponse<_>>(&endpoint, "getBundleStatuses", json!([ids]))
            .await?
            .result)
    }

    /// Get the in-flight status for up to a maximum of 5 bundles.
    pub async fn get_inflight_bundle_statuses(
        &self,
        ids: &[String],
        region_override: Option<JitoRegion>,
    ) -> Result<WithContext<Vec<Value>>> {
        let endpoint = format!(
            "{}{}",
            region_override.unwrap_or(self.region).urls().url,
            GET_INFLIGHT_BUNDLE_STATUSES
        );

        Ok(self
            .make_request::<JitoResponse<_>>(&endpoint, "getInflightBundleStatuses", json!([ids]))
            .await?
            .result)
    }

    async fn make_request<T>(&self, url: &str, method: &'static str, params: Value) -> Result<T>
    where
        T: de::DeserializeOwned,
    {
        let json = &json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        println!("request: {:#?}", json);
        let response = self
            .client
            .post(url)
            .header("Content-Type", "application/json")
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await;

        let response = match response {
            Ok(response) => response,
            Err(err) => return Err(anyhow!("fail to send request: {err}")),
        };

        let status = response.status();
        let text = match response.text().await {
            Ok(text) => text,
            Err(err) => return Err(anyhow!("fail to read response content: {err:#}")),
        };
        println!("text: {}", text);

        if !status.is_success() {
            return Err(anyhow!("status code: {status}, response: {text}"));
        }

        let response: T = match serde_json::from_str(&text) {
            Ok(response) => response,
            Err(err) => {
                return Err(anyhow!(
                    "fail to deserialize response: {err:#}, response: {text}, status: {status}"
                ));
            }
        };

        Ok(response)
    }
}

pub const JITO_TIP_ACCOUNTS: [Pubkey; 8] = [
    pubkey!("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
    pubkey!("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe"),
    pubkey!("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY"),
    pubkey!("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49"),
    pubkey!("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh"),
    pubkey!("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt"),
    pubkey!("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL"),
    pubkey!("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"),
];

pub fn find_jito_tip<'a>(
    account_keys: &[Pubkey],
    mut instructions: impl Iterator<Item = (&'a Pubkey, SVMInstruction<'a>)>,
) -> Option<u64> {
    instructions.find_map(|(program, ix)| {
        if *program != system_program::ID {
            return None;
        }
        match limited_deserialize(ix.data, std::mem::size_of::<SystemInstruction>() as u64) {
            Ok(SystemInstruction::Transfer { lamports }) => {
                let account = account_keys.get(ix.accounts[1] as usize)?;
                JITO_TIP_ACCOUNTS.contains(account).then_some(lamports)
            }
            _ => None,
        }
    })
}

pub struct BlockEngine<'a> {
    pub url: &'a str,
    #[allow(dead_code)]
    pub shred_receiver_addr: &'a str,
    #[allow(dead_code)]
    pub relayer_url: &'a str,
}

pub const JITO_BLOCK_ENGINES: [BlockEngine; 7] = [
    BlockEngine {
        url: "https://amsterdam.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "74.118.140.240:1002",
        relayer_url: "http://amsterdam.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://frankfurt.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "145.40.93.84:1002",
        relayer_url: "http://frankfurt.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://london.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "142.91.127.175:1002",
        relayer_url: "http://london.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://ny.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "141.98.216.96:1002",
        relayer_url: "http://ny.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://slc.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "64.130.53.8:1002",
        relayer_url: "http://slc.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://singapore.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "202.8.11.224:1002",
        relayer_url: "http://singapore.mainnet.relayer.jito.wtf:8100",
    },
    BlockEngine {
        url: "https://tokyo.mainnet.block-engine.jito.wtf",
        shred_receiver_addr: "202.8.9.160:1002",
        relayer_url: "http://tokyo.mainnet.relayer.jito.wtf:8100",
    },
];

pub const BUNDLE_ENDPOINT: &str = "/api/v1/bundles";
pub const TRANSACTIONS_ENDPOINT: &str = "/api/v1/transactions";

pub fn pick_jito_recipient() -> &'static Pubkey {
    &JITO_TIP_ACCOUNTS[rand::rng().random_range(0..JITO_TIP_ACCOUNTS.len())]
}

fn serialize_and_encode<T>(input: &T, encoding: UiTransactionEncoding) -> Result<String>
where
    T: serde::ser::Serialize,
{
    let serialized = bincode::serialize(input).map_err(|e| anyhow!("Serialization failed: {e}"))?;
    let encoded = match encoding {
        UiTransactionEncoding::Base58 => solana_sdk::bs58::encode(serialized).into_string(),
        UiTransactionEncoding::Base64 => BASE64_STANDARD.encode(serialized),
        _ => {
            return Err(anyhow!(
                "unsupported encoding: {encoding}. Supported encodings: base58, base64"
            ));
        }
    };
    Ok(encoded)
}

#[derive(Debug, Clone, Deserialize)]
pub struct WithContext<T> {
    pub context: Context,
    pub value: T,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Context {
    pub slot: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BundleStatus {
    pub bundle_id: String,
    pub transactions: Vec<String>,
    pub slot: u64,
    pub confirmation_status: TransactionConfirmationStatus,
    pub err: Value,
}

impl BundleStatus {
    pub fn satisfies_commitment(&self, commitment_config: CommitmentConfig) -> bool {
        use TransactionConfirmationStatus::*;
        match commitment_config.commitment {
            CommitmentLevel::Finalized => matches!(self.confirmation_status, Finalized),
            CommitmentLevel::Confirmed => matches!(self.confirmation_status, Confirmed | Finalized),
            CommitmentLevel::Processed => {
                matches!(self.confirmation_status, Processed | Confirmed | Finalized)
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct InflightBundleStatus {
    pub bundle_id: String,
    pub status: Status,
    pub landed_slot: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    /// The bundle ID is not found in the system (5-minute look back)
    Invalid,
    /// The bundle has not failed, landed, or been marked invalid
    Pending,
    /// All regions have marked the bundle as failed and it hasn't been forwarded
    Failed,
    /// Bundle successfully landed on-chain
    Landed,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct JitoTips {
    #[serde(rename = "landed_tips_25th_percentile")]
    pub p25_landed: f64,

    #[serde(rename = "landed_tips_50th_percentile")]
    pub p50_landed: f64,

    #[serde(rename = "landed_tips_75th_percentile")]
    pub p75_landed: f64,

    #[serde(rename = "landed_tips_95th_percentile")]
    pub p95_landed: f64,

    #[serde(rename = "landed_tips_99th_percentile")]
    pub p99_landed: f64,
}

impl JitoTips {
    pub fn p75(&self) -> u64 {
        (self.p75_landed * 1e9f64) as u64
    }

    pub fn p50(&self) -> u64 {
        (self.p50_landed * 1e9f64) as u64
    }

    pub fn p25(&self) -> u64 {
        (self.p25_landed * 1e9f64) as u64
    }

    pub fn p95(&self) -> u64 {
        (self.p95_landed * 1e9f64) as u64
    }

    pub fn p99(&self) -> u64 {
        (self.p99_landed * 1e9f64) as u64
    }
}

impl std::fmt::Display for JitoTips {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tips(p25={},p50={},p75={},p95={},p99={})",
            (self.p25_landed * 1e9f64) as u64,
            (self.p50_landed * 1e9f64) as u64,
            (self.p75_landed * 1e9f64) as u64,
            (self.p95_landed * 1e9f64) as u64,
            (self.p99_landed * 1e9f64) as u64
        )
    }
}

pub fn subscribe_jito_tips(
    tips: Arc<RwLock<JitoTips>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn({
        let tips = tips.clone();
        async move {
            let url = "wss://bundles.jito.wtf/api/v1/bundles/tip_stream";

            loop {
                let stream = match tokio_tungstenite::connect_async(url).await {
                    Ok((ws_stream, _)) => ws_stream,
                    Err(err) => {
                        error!("fail to connect to jito tip stream: {err:#}");
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        continue;
                    }
                };

                let (_, read) = stream.split();

                read.for_each(|message| async {
                    let data = match message {
                        Ok(data) => data.into_data(),
                        Err(err) => {
                            error!("fail to read jito tips message: {err:#}");
                            return;
                        }
                    };

                    let data = match serde_json::from_slice::<Vec<JitoTips>>(&data) {
                        Ok(t) => t,
                        Err(err) => {
                            debug!("fail to parse jito tips: {err:#}");
                            return;
                        }
                    };

                    if data.is_empty() {
                        return;
                    }

                    let tip = data.first().unwrap();
                    log::trace!("tip: {}", tip);
                    *tips.write().await = *tip;
                })
                .await;

                info!("jito tip stream disconnected, retries in 5 seconds");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }

            #[allow(unreachable_code)]
            Ok(())
        }
    })
}

pub async fn jito_tip_stream(tips: Arc<RwLock<JitoTips>>) {
    subscribe_jito_tips(tips.clone());
    loop {
        let tips = *tips.read().await;
        debug!("tips: {}", tips);
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }
}
