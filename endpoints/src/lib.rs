pub mod bootstrap;
mod defaults;
pub mod grpc;
pub mod ids;
pub mod jito_rpc;
pub mod metrics;
pub mod rpc;
pub mod send_txn;
pub mod types;

use anyhow::Context;
use builder_rs::Builder;
use graceful_core::transaction::TxWithMetadata;
use helius_laserstream::grpc::SubscribeUpdateTransaction;
use serde::Deserialize;
use solana_account::Account;

pub use ids::*;
pub use send_txn::types::*;
pub use types::*;

use std::sync::{Arc, atomic::AtomicU64};

use dashmap::DashMap;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;
use solana_sdk::{commitment_config::CommitmentConfig, signature::Signature};
use tokio::sync::RwLock;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use crate::{
    bootstrap::{RpcData, bootstrap_endpoint_data},
    jito_rpc::{JitoTips, subscribe_jito_tips},
};

/// Solana cluster information
#[derive(Clone)]
pub struct ClusterEndpoints {
    /// RPC Client
    pub rpc: Arc<RpcClient>,

    /// Recent blockhash for sending transactions
    pub recent_blockhash: Arc<RwLock<BlockHashUpdate>>,

    /// The highest block-height we know about. Used in transaction confirmation
    pub current_block_height: Arc<AtomicU64>,

    /// The most current slot we know about
    pub current_slot: Arc<RwLock<SlotUpdate>>,

    /// The most recent state of pre-specified accounts
    pub accounts: Arc<DashMap<Pubkey, Account>>,

    /// Jito tip stream
    pub jito_tips: Arc<RwLock<JitoTips>>,
}

/// Channels for streaming data
pub struct ClusterReceivers {
    /// Streams slot updates
    pub slot_streamer: Option<BroadcastReceiver<SlotUpdate>>,

    /// Streams transaction statuses
    pub tx_status_streamer: Option<BroadcastReceiver<TransactionStatus>>,

    /// Streams transactions
    pub tx_streamer: Option<BroadcastReceiver<ClusterTx>>,
}

#[derive(Clone, Debug)]
pub enum ClusterTx {
    Grpc(SubscribeUpdateTransaction),
    Converted(TxWithMetadata),
}

impl ClusterTx {
    pub fn signature(&self) -> Option<Signature> {
        match self {
            ClusterTx::Converted(tx) => Some(tx.signature()),
            ClusterTx::Grpc(tx) => tx
                .transaction
                .as_ref()
                .and_then(|t| {
                    Some(
                        t.transaction
                            .as_ref()?
                            .signatures
                            .first()
                            .and_then(|key| Signature::try_from(key.as_slice()).ok()),
                    )
                })
                .unwrap_or_default(),
        }
    }

    pub fn signers(&self) -> Vec<Pubkey> {
        match self {
            ClusterTx::Converted(tx) => tx.signers().to_vec(),
            ClusterTx::Grpc(tx) => tx
                .transaction
                .as_ref()
                .and_then(|t| {
                    Some(
                        t.transaction
                            .as_ref()?
                            .message
                            .as_ref()?
                            .account_keys
                            .iter()
                            .flat_map(|key| Pubkey::try_from(key.as_slice()).ok())
                            .collect::<Vec<_>>(),
                    )
                })
                .unwrap_or_default(),
        }
    }

    pub fn is_error(&self) -> bool {
        match self {
            ClusterTx::Converted(tx) => tx.meta.err.is_some(),
            ClusterTx::Grpc(tx) => tx
                .transaction
                .as_ref()
                .and_then(|tx| Some(tx.meta.as_ref()?.err.is_some()))
                .unwrap_or(true),
        }
    }
}

impl TryFrom<ClusterTx> for TxWithMetadata {
    type Error = anyhow::Error;

    #[inline]
    fn try_from(value: ClusterTx) -> Result<Self, Self::Error> {
        match value {
            ClusterTx::Grpc(tx) => tx.try_into(),
            ClusterTx::Converted(tx) => Ok(tx),
        }
    }
}

impl From<SubscribeUpdateTransaction> for ClusterTx {
    fn from(value: SubscribeUpdateTransaction) -> Self {
        ClusterTx::Grpc(value)
    }
}

impl From<TxWithMetadata> for ClusterTx {
    fn from(value: TxWithMetadata) -> Self {
        ClusterTx::Converted(value)
    }
}

#[derive(Builder, Debug, Clone, Default, Deserialize)]
pub struct EndpointConfig {
    rpc_url: Option<String>,
    grpc_url: Option<String>,
    grpc_x_token: Option<String>,
    poll_slot_frequency_ms: Option<u64>,
    poll_blockhash_frequency_ms: Option<u64>,
    poll_block_height_frequency_ms: Option<u64>,
    poll_accounts_frequency_ms: Option<u64>,
    update_accounts: Vec<Pubkey>,
    subscribe_txs: Option<TxSubscribe>,
    subscribe_tx_statuses: Option<TxSubscribe>,
    subscribe_jito_tip: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub enum TxSubscribe {
    All,
    Some(Vec<String>),
}

#[derive(Builder, Clone, Copy, Default, Deserialize)]
pub struct ClusterConfig {
    commitment: CommitmentConfig,
    update_accounts: bool,
    update_blockhash: bool,
    update_blockheight: bool,
    update_slot: bool,
    notify_slot: bool,
    notify_txn_status: bool,
    notify_txn: bool,
}

impl EndpointConfig {
    pub async fn build_recommended(
        self,
        bootstrap_attempts: u8,
        notify_slot: bool,
    ) -> anyhow::Result<(ClusterEndpoints, ClusterReceivers)> {
        let processed_config = ClusterConfig {
            commitment: CommitmentConfig::processed(),
            notify_slot,
            update_slot: true,
            update_blockheight: true,
            notify_txn: true,
            notify_txn_status: true,
            update_accounts: false,
            update_blockhash: false,
        };
        let confirmed_config = ClusterConfig {
            commitment: CommitmentConfig::confirmed(),
            notify_slot: false,
            update_slot: false,
            update_blockheight: false,
            notify_txn: false,
            notify_txn_status: false,
            update_accounts: true,
            update_blockhash: true,
        };

        let rpc_url = self.rpc_url.clone().context("RPC URL not set")?;
        let rpc_client = RpcClient::new(rpc_url);
        let bootstrap =
            bootstrap_endpoint_data(&rpc_client, &self.update_accounts, bootstrap_attempts).await?;

        let (processed_endpoint, processed_receivers) = self
            .clone()
            .build_loaded_with_config_inner(bootstrap.clone(), processed_config)?;
        let (confirmed_endpoint, _confirmed_receivers) = self
            .clone()
            .build_loaded_with_config_inner(bootstrap.clone(), confirmed_config)?;

        let info = ClusterEndpoints {
            rpc: confirmed_endpoint.rpc.clone(),
            accounts: confirmed_endpoint.accounts.clone(),
            recent_blockhash: confirmed_endpoint.recent_blockhash.clone(),
            ..processed_endpoint
        };

        if self.subscribe_jito_tip {
            subscribe_jito_tips(info.jito_tips.clone());
        }

        Ok((info, processed_receivers))
    }

    pub async fn build_with_config(
        self,
        bootstrap_attempts: u8,
        config: ClusterConfig,
    ) -> anyhow::Result<(ClusterEndpoints, ClusterReceivers)> {
        let rpc_url = self.rpc_url.clone().context("RPC URL not set")?;
        let rpc_client = RpcClient::new(rpc_url);

        let bootstrap =
            bootstrap_endpoint_data(&rpc_client, &self.update_accounts, bootstrap_attempts).await?;
        self.build_loaded_with_config(bootstrap, config)
    }

    fn build_loaded_with_config_inner(
        self,
        bootstrap: RpcData,
        config: ClusterConfig,
    ) -> anyhow::Result<(ClusterEndpoints, ClusterReceivers)> {
        let inner = if self.grpc_url.is_some() {
            grpc::update_task::start_grpc_task(self, config, bootstrap)?
        } else {
            rpc::update_task::start_rpc_task(self, config, bootstrap)?
        };

        Ok(inner)
    }

    pub fn build_loaded_with_config(
        self,
        bootstrap: RpcData,
        config: ClusterConfig,
    ) -> anyhow::Result<(ClusterEndpoints, ClusterReceivers)> {
        self.build_loaded_with_config_inner(bootstrap, config)
    }
}
