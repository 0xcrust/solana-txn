use crate::jito_rpc::{JITO_TIP_ACCOUNTS, JitoClient};
use crate::{BlockHashUpdate, FeeInfo, PendingTransaction, TransactionId, TransactionSourceId};
use std::time::Instant;

use anyhow::Context;
use graceful_core::{
    priority::PriorityFee,
    tx_ext::{ComputeMods, TransactionMods},
};
use itertools::Itertools;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use solana_client::rpc_client::SerializableTransaction;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::signature::Signature;
use solana_sdk::signer::Signer;
use solana_sdk::signer::keypair::Keypair;
use solana_sdk::transaction::VersionedTransaction;
use std::sync::Arc;

/// Either a single or bundled transaction
#[derive(Clone, Debug)]
pub struct SendTransactionData {
    /// Unique id
    pub id: TransactionId,

    /// Source id
    pub source_id: TransactionSourceId,

    /// Blockhash validity period
    pub last_valid_block_height: u64,

    /// Transaction data
    pub data: SingleOrBundled,

    /// Transaction send time
    pub sent_at: Instant,

    /// The slot the transaction was attempted at
    pub send_slot: u64,

    /// Number of times this transaction has been attempted
    pub attempts: u8,

    /// Configuration values
    pub config: SendTxConfig,
}

#[derive(Clone, Copy, Debug, Deserialize)]
pub struct SendTxConfig {
    /// send to rpc
    pub use_rpc: bool,

    /// send to jito
    pub use_jito: bool,

    /// number of retries
    pub retries: u8,

    /// cu-price increment per attempt.
    ///
    /// For a bundle, this increment is applied for each transaction in the bundle.
    pub cu_price_increment: u64,

    /// tip increment per attempt.
    ///
    /// For a bundle, this increment is applied once for the entire bundle
    pub tip_increment: u64,

    /// RPC config
    pub rpc_config: Option<RpcSendTransactionConfig>,
}

impl Default for SendTxConfig {
    fn default() -> Self {
        SendTxConfig {
            use_rpc: true,
            use_jito: false,
            retries: 0,
            cu_price_increment: 0,
            tip_increment: 0,
            rpc_config: None
        }
    }
}

impl SendTransactionData {
    pub fn new_single(
        tx: VersionedTransaction,
        signers: &[Arc<Keypair>],
        source_id: TransactionSourceId,
        config: SendTxConfig,
    ) -> Self {
        let sent_at = Instant::now();
        let id = TransactionId::next();

        SendTransactionData {
            id,
            source_id,
            data: SingleOrBundled::new_single(tx, signers),
            last_valid_block_height: 0,
            sent_at,
            send_slot: 0,
            attempts: 0,
            config,
        }
    }

    pub fn new_bundled(
        txs: Vec<(VersionedTransaction, &[Arc<Keypair>])>,
        source_id: TransactionSourceId,
        config: SendTxConfig,
    ) -> Self {
        let sent_at = Instant::now();
        let id = TransactionId::next();

        SendTransactionData {
            id,
            source_id,
            data: SingleOrBundled::new_bundled(txs),
            last_valid_block_height: 0,
            sent_at,
            send_slot: 0,
            attempts: 0,
            config,
        }
    }

    pub fn update(mut self, blockhash: BlockHashUpdate, slot: u64) -> anyhow::Result<Option<Self>> {
        if self.attempts > self.config.retries {
            return Ok(None);
        }

        self.sent_at = Instant::now();
        self.send_slot = slot;
        self.attempts += 1;
        self.last_valid_block_height = blockhash.last_valid_block_height;

        self.data = self.data.finalize(
            blockhash,
            self.config.cu_price_increment,
            self.config.tip_increment,
        )?;

        Ok(Some(self))
    }

    pub fn is_single(&self) -> bool {
        matches!(self.data, SingleOrBundled::Single(_))
    }

    pub fn is_bundled(&self) -> bool {
        matches!(self.data, SingleOrBundled::Bundled(_))
    }

    pub fn as_pending(&self, bundle_tracking_only: bool) -> PendingTransaction {
        PendingTransaction {
            id: self.id,
            last_valid_block_height: self.last_valid_block_height,
            bundle_tracking_only,
            source: self.source_id,
            created_at: Instant::now(),
        }
    }

    pub fn is_first_send(&self) -> bool {
        self.attempts == 1
    }

    pub fn can_reschedule(&self) -> bool {
        self.attempts < self.config.retries + 1
    }

    pub fn tx_count(&self) -> usize {
        match &self.data {
            SingleOrBundled::Bundled(data) => data.bundle.len(),
            SingleOrBundled::Single(_) => 1,
        }
    }

    pub fn fee(&self) -> FeeInfo {
        match &self.data {
            SingleOrBundled::Single(data) => FeeInfo {
                priority: data.fee.fee,
                tip: data.fee.tip,
            },
            SingleOrBundled::Bundled(data) => FeeInfo {
                priority: data
                    .bundle
                    .iter()
                    .map(|tx| tx.fee)
                    .map(|f| f.fee)
                    .sum::<u64>(),
                tip: data
                    .bundle
                    .iter()
                    .map(|tx| tx.fee)
                    .map(|f| f.tip)
                    .sum::<u64>(),
            },
        }
    }

    pub fn derive_bundle_id(&self) -> anyhow::Result<String> {
        match &self.data {
            SingleOrBundled::Bundled(data) => {
                derive_bundle_id(data.bundle.iter().map(|data| &data.tx))
            }
            SingleOrBundled::Single(data) => derive_bundle_id(std::iter::once(&data.tx)),
        }
    }
}

/// Either a single or bundled transaction
#[derive(Clone, Debug)]
pub enum SingleOrBundled {
    /// Bundled transaction
    Bundled(BundleTxData),

    /// Single transaction
    Single(Box<TransactionData>),
}

impl SingleOrBundled {
    /// Create a single transaction
    pub fn new_single(tx: VersionedTransaction, signers: &[Arc<Keypair>]) -> Self {
        SingleOrBundled::Single(Box::new(TransactionData::new(tx, signers)))
    }

    /// Create a bundle of transactions
    pub fn new_bundled(txs: Vec<(VersionedTransaction, &[Arc<Keypair>])>) -> Self {
        SingleOrBundled::Bundled(BundleTxData::new(txs))
    }

    /// Prepares `Self` to be sent over the network.
    ///
    /// The following actions are taken:
    ///
    /// * updates `blockhash` and/or `last_valid_block_height` for the txn/bundle
    /// * increments the number of attempts
    /// * increments the `cu-price` and/or `tip-amount` of each transaction
    /// * re-signs each transaction
    /// * updates the `sent-at-slot` value
    /// * updates other internal params such as the tracked priority values
    pub fn finalize(
        self,
        blockhash: BlockHashUpdate,
        cu_inc: u64,
        tip_inc: u64,
    ) -> anyhow::Result<Self> {
        match self {
            Self::Bundled(data) => data
                .finalize(blockhash.blockhash, cu_inc, tip_inc)
                .map(Self::Bundled),
            Self::Single(data) => data
                .finalize(blockhash.blockhash, cu_inc, tip_inc)
                .map(|data| Self::Single(Box::new(data))),
        }
    }
}

/// Transaction-specific data
#[derive(Clone, Debug)]
pub struct TransactionData {
    /// Transaction signature
    pub signature: Signature,

    /// Transaction payload
    pub tx: VersionedTransaction,

    /// Latest priority fee for this transaction
    pub fee: PriorityFee,

    /// Signers for this transaction
    pub signers: Vec<Arc<Keypair>>,
}

impl TransactionData {
    /// Creates a new instance of `Self`
    pub fn new(tx: VersionedTransaction, signers: &[Arc<Keypair>]) -> Self {
        TransactionData {
            signature: *tx.get_signature(),
            fee: PriorityFee::from_versioned_transaction(&tx, &JITO_TIP_ACCOUNTS),
            tx,
            signers: signers.to_vec(),
        }
    }

    /// Updates `Self`, performing transformations to the tx payload and other related fields.
    ///
    /// Performs the following actions:
    ///
    /// * updates the `blockhash`
    /// * re-signs the transaction
    /// * increments the `cu-price` and `tip-amount` if needed
    /// * updates the transaction signature
    /// * updates the prio-fee field
    pub fn finalize(
        mut self,
        blockhash: Hash,
        cu_increment: u64,
        tip_increment: u64,
    ) -> anyhow::Result<Self> {
        if cu_increment > 0 {
            let cu_price = self.fee.cu_price + cu_increment;
            if !self.tx.message.update_cu(cu_price) {
                // insert if not found
                let ix = ComputeBudgetInstruction::set_compute_unit_price(cu_price);
                self.tx.message.insert_ix(0, ix);
            }
        }

        if tip_increment > 0 {
            let tip = self.fee.tip + tip_increment;

            if !self.tx.message.update_tip(tip, &JITO_TIP_ACCOUNTS) {
                // insert if not found
                let signer = self.signers[0].pubkey();
                let ix = JitoClient::build_bribe_ix(&signer, tip);
                self.tx
                    .message
                    .insert_ix(self.tx.message.instructions().len(), ix);
            }
        }

        if cu_increment > 0 || tip_increment > 0 {
            self.fee = PriorityFee::from_versioned_transaction(&self.tx, &JITO_TIP_ACCOUNTS);
        }

        self.tx.message.set_recent_blockhash(blockhash);
        let tx = VersionedTransaction::try_new(self.tx.message, &self.signers)?;

        self.signature = *tx.get_signature();
        self.tx = tx;

        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct BundleTxData {
    /// The bundle id
    pub uuid: String,

    /// The transactions in the bundle
    pub bundle: Vec<TransactionData>,
}

impl BundleTxData {
    /// Create a new instance of `Self`
    pub fn new(txs: Vec<(VersionedTransaction, &[Arc<Keypair>])>) -> Self {
        let bundle = txs
            .into_iter()
            .map(|(tx, signers)| TransactionData::new(tx, signers))
            .collect::<Vec<_>>();

        BundleTxData {
            uuid: derive_bundle_id(bundle.iter().map(|data| &data.tx)).unwrap_or_default(),
            bundle,
        }
    }

    /// Prepares `Self` to be sent over the network.
    pub fn finalize(mut self, blockhash: Hash, cu_inc: u64, tip_inc: u64) -> anyhow::Result<Self> {
        if self.bundle.is_empty() {
            unreachable!("bug: bundle should be non-empty")
        }

        self.uuid = derive_bundle_id(self.bundle.iter().map(|data| &data.tx))?;

        // only update tip for first transaction in the bundle
        let mut finalized_bundles = Vec::with_capacity(self.bundle.len());
        finalized_bundles.push(self.bundle.remove(0).finalize(blockhash, cu_inc, tip_inc)?);

        let rest = self
            .bundle
            .into_iter()
            .map(|tx| tx.finalize(blockhash, cu_inc, tip_inc))
            .collect::<anyhow::Result<Vec<_>>>()?;
        finalized_bundles.extend(rest);

        self.bundle = finalized_bundles;

        Ok(self)
    }
}

/// Derives a bundle id from signatures, returning error if signature is missing
pub fn derive_bundle_id<'a>(
    transactions: impl Iterator<Item = &'a VersionedTransaction>,
) -> anyhow::Result<String> {
    let signatures = transactions
        .enumerate()
        .map(|(idx, tx)| {
            tx.signatures
                .first()
                .context(format!("missing signature for index {}", idx))
        })
        .collect::<anyhow::Result<Vec<_>, _>>()?;

    let mut hasher = Sha256::new();
    hasher.update(signatures.iter().join(","));
    Ok(format!("{:x}", hasher.finalize()))
}
