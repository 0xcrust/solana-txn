use std::time::Duration;

use solana_sdk::commitment_config::CommitmentConfig;

pub const RPC_URL: &str = "https://api.mainnet-beta.solana.com";

/// Default number of concurrent get-transaction requests
pub const GET_TXN_CONCURRENCY: usize = 20;

/// Default commitments
pub const COMMITMENT: CommitmentConfig = CommitmentConfig::confirmed();

/// How frequently to check confirmation over RPC
pub const CHECK_CONFIRMATION_FREQUENCY_RPC: Duration = Duration::from_secs(2);

/// How frequently to check expiry over GRPC
pub const CHECK_CONFIRMATION_FREQUENCY_GRPC: Duration = Duration::from_secs(2);

/// How frequently to check for a new slot
pub const POLL_SLOT_FREQUENCY: Duration = Duration::from_secs(1);

/// How frequently to check for a new blockhash
pub const POLL_BLOCKHASH_FREQUENCY: Duration = Duration::from_secs(1);

/// How frequently to refresh accounts
pub const POLL_ACCOUNTS_FREQUENCY: Duration = Duration::from_secs(1);

/// How long a solana slot lasts(approximately)
pub const AVERAGE_SLOT_CHANGE_TIME: Duration = Duration::from_millis(380);

pub const SLOT_STREAMER_CAPACITY: usize = 100;

pub const TX_STREAMER_CAPACITY: usize = 100;
