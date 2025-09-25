use crate::{TransactionId, TransactionSourceId};

use solana_sdk::clock::Slot;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use std::time::Instant;

/// Represents a transaction waiting to be confirmed
pub struct PendingTransaction {
    /// The unique id
    pub id: TransactionId,
    /// Blockhash expiration
    pub last_valid_block_height: u64,
    /// This is only tracked for extra bundle-bookkeeping purposes and
    /// won't be used to determine a transaction's confirmation
    pub bundle_tracking_only: bool,
    /// The transaction source id
    pub source: TransactionSourceId,
    /// Creation time of this object
    pub created_at: Instant,
}

impl PendingTransaction {
    fn is_expired(&self, current_block_height: u64) -> bool {
        current_block_height > self.last_valid_block_height
    }

    fn is_not_expired(&self, current_block_height: u64) -> bool {
        !self.is_expired(current_block_height)
    }

    fn is_recently_expired(&self, current_block_height: u64, last_block_height: u64) -> bool {
        self.is_not_expired(last_block_height) && self.is_expired(current_block_height)
    }

    /// Transaction is not expired or recently expired between `current-block-height` and `last-block-height`
    pub fn validate(&self, current_block_height: u64, last_block_height: u64) -> bool {
        self.is_not_expired(current_block_height)
            || self.is_recently_expired(current_block_height, last_block_height)
    }

    /// Transaction is not expired
    pub fn validate_strict(&self, current_block_height: u64) -> bool {
        self.is_not_expired(current_block_height)
    }
}

/// Transaction execution result
#[derive(Clone, Debug)]
pub struct ExecutionResult {
    /// Id for this bundle/txn
    pub id: TransactionId,
    /// The processed info, if this txn/bundle landed on-chain
    pub processed: Option<Processed>,
    /// The signature for this execution
    pub signature: String,
    /// If this is a transaction or bundle
    pub bundled: bool,
    /// Number of attempts before landing/retry-expiry
    pub attempts: u8,
    /// Number of txns executed
    pub tx_count: usize,
    /// Transaction error, if on-chain execution failed
    pub error: Option<TransactionError>,
    /// Fees paid
    pub fee: FeeInfo,
}

#[derive(Clone, Debug)]
pub struct Processed {
    /// The processed slot for this txn/bundle
    pub slot: u64,
}

impl Processed {
    pub fn new(slot: u64) -> Processed {
        Processed { slot }
    }
}

#[derive(Clone, Debug)]
pub struct FeeInfo {
    pub tip: u64,
    pub priority: u64,
}

#[derive(Debug, Clone)]
pub struct ConfirmationResult {
    pub id: TransactionId,
    pub uuid: String,
    pub status: ConfirmationStatus,
    pub bundle_tracking_only: bool,
}

#[derive(Debug, Clone)]
pub enum ConfirmationStatus {
    /// Transaction landed
    Confirmed {
        slot: u64,
        error: Option<TransactionError>,
    },
    /// Transaction expired
    Expired,
}

#[derive(Debug, Clone)]
pub struct TransactionStatus {
    pub signature: Signature,
    pub error: Option<TransactionError>,
    pub slot: u64,
}

impl TransactionStatus {
    pub fn new(signature: Signature, error: Option<TransactionError>, slot: u64) -> Self {
        Self {
            signature,
            error,
            slot,
        }
    }
}

/// Slot update
#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub slot: Slot,
    #[allow(unused)]
    pub instant: Instant,
}

impl SlotUpdate {
    pub fn new(slot: Slot) -> Self {
        Self {
            slot,
            instant: Instant::now(),
        }
    }
}

/// Blockhash update
#[derive(Clone)]
pub struct BlockHashUpdate {
    pub blockhash: Hash,
    pub last_valid_block_height: u64,
}

impl BlockHashUpdate {
    pub fn new(blockhash: Hash, last_valid_block_height: u64) -> Self {
        Self {
            blockhash,
            last_valid_block_height,
        }
    }
}
