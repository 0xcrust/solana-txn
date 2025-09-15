use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, bail};
use dashmap::DashMap;
use futures::StreamExt;
use helius_laserstream::{
    LaserstreamConfig,
    grpc::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateAccountInfo,
        subscribe_update::UpdateOneof,
    },
    subscribe,
};
use log::{info, trace};
use solana_account::Account;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    clock::MAX_PROCESSING_AGE, hash::Hash, pubkey::Pubkey, signature::Signature,
    transaction::TransactionError,
};
use tokio::sync::{
    RwLock,
    broadcast::{self, Sender},
};

use crate::{
    BlockHashUpdate, ClusterConfig, ClusterEndpoints, ClusterReceivers, ClusterTx, EndpointConfig,
    SlotUpdate, TransactionStatus, TxSubscribe,
    bootstrap::RpcData,
    defaults::{RPC_URL, SLOT_STREAMER_CAPACITY, TX_STREAMER_CAPACITY},
    jito_rpc::JitoTips,
};

pub(crate) fn start_grpc_task(
    endpoint_config: EndpointConfig,
    cluster_config: ClusterConfig,
    bootstrap: RpcData,
) -> anyhow::Result<(ClusterEndpoints, ClusterReceivers)> {
    let Some(grpc_url) = endpoint_config.grpc_url else {
        bail!("grpc URL not specified");
    };

    let Some(x_token) = endpoint_config.grpc_x_token else {
        bail!("grpc X-token not provided");
    };

    let rpc_url = endpoint_config.rpc_url.unwrap_or(RPC_URL.to_string());
    let rpc = Arc::new(RpcClient::new(rpc_url));

    let ClusterConfig {
        commitment,
        notify_slot,
        notify_txn_status,
        notify_txn,
        update_accounts,
        update_blockhash,
        update_blockheight,
        update_slot,
    } = cluster_config;

    let mut something_to_do = false;

    let laserstream_config = LaserstreamConfig {
        api_key: x_token,
        endpoint: grpc_url,
        ..Default::default()
    };

    let mut subscribe_request = SubscribeRequest {
        commitment: Some(commitment.commitment as i32),
        ..Default::default()
    };
    let disc = rand::random::<u32>();

    if update_slot || notify_slot {
        something_to_do = true;
    }

    if update_blockhash || update_blockheight {
        let blocks_filter = SubscribeRequestFilterBlocksMeta {};
        subscribe_request.blocks_meta = HashMap::from_iter(vec![(
            format!("graceful-blocks-meta-{disc}"),
            blocks_filter,
        )]);
        something_to_do = true;
    }

    if update_accounts && !endpoint_config.update_accounts.is_empty() {
        let accounts_filter = SubscribeRequestFilterAccounts {
            account: endpoint_config
                .update_accounts
                .clone()
                .into_iter()
                .map(|a| a.to_string())
                .collect(),
            ..Default::default()
        };
        subscribe_request.accounts = HashMap::from_iter(vec![(
            format!("graceful-blocks-meta-{disc}"),
            accounts_filter,
        )]);
        something_to_do = true;
    }

    let (status_tx, status_rx) = if notify_txn_status {
        let accounts = match endpoint_config.subscribe_tx_statuses.as_ref() {
            Some(TxSubscribe::Some(accounts)) => accounts.clone(),
            Some(TxSubscribe::All) | None => vec![],
        };

        let mut transactions_status = HashMap::new();
        let transactions_status_filter = SubscribeRequestFilterTransactions {
            account_required: accounts,
            ..Default::default()
        };
        transactions_status.insert(
            format!("graceful-tx-status-{disc}"),
            transactions_status_filter,
        );
        subscribe_request.transactions_status = transactions_status;
        something_to_do = true;

        let (tx, rx) = broadcast::channel::<TransactionStatus>(TX_STREAMER_CAPACITY);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let (tx_tx, tx_rx) = if notify_txn {
        let accounts = match endpoint_config.subscribe_txs.as_ref() {
            Some(TxSubscribe::Some(accounts)) => accounts.clone(),
            Some(TxSubscribe::All) | None => vec![],
        };

        let mut transactions = HashMap::new();
        let transactions_filter = SubscribeRequestFilterTransactions {
            account_required: accounts,
            ..Default::default()
        };
        transactions.insert(format!("graceful-tx-{disc}"), transactions_filter);
        subscribe_request.transactions = transactions;
        something_to_do = true;

        let (tx, rx) = broadcast::channel::<ClusterTx>(TX_STREAMER_CAPACITY);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    if !something_to_do {
        bail!("bug: nothing to do for grpc task");
    }

    let (slot_tx, slot_rx) = if notify_slot {
        let (tx, rx) = broadcast::channel::<SlotUpdate>(SLOT_STREAMER_CAPACITY);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let recent_blockhash = Arc::new(RwLock::new(bootstrap.current_blockhash));
    let current_block_height = Arc::new(AtomicU64::new(bootstrap.current_block_height));
    let current_slot = Arc::new(RwLock::new(bootstrap.current_slot));
    let accounts = Arc::new(DashMap::new());

    let cluster_info = ClusterEndpoints {
        rpc,
        recent_blockhash: recent_blockhash.clone(),
        current_block_height: current_block_height.clone(),
        current_slot: current_slot.clone(),
        accounts: accounts.clone(),
        jito_tips: Arc::new(RwLock::new(JitoTips::default())),
    };

    let receivers = ClusterReceivers {
        slot_streamer: slot_rx,
        tx_streamer: tx_rx,
        tx_status_streamer: status_rx,
    };

    tokio::task::spawn({
        async move {
            info!("Connecting and subscribing...");
            let (stream, _grpc_subscription) = subscribe(laserstream_config, subscribe_request);
            info!("GRPC connection successful");

            futures::pin_mut!(stream);
            while let Some(result) = stream.next().await {
                let update = match result {
                    Ok(res) => res,
                    Err(e) => {
                        log::error!("grpc error: {}", e);
                        continue;
                    }
                };
                if let Err(e) = process_grpc_update(
                    update,
                    &cluster_config,
                    &recent_blockhash,
                    &current_block_height,
                    &current_slot,
                    &accounts,
                    &slot_tx,
                    &status_tx,
                    &tx_tx,
                )
                .await
                {
                    log::error!("error processing update: {}", e)
                }
            }
        }
    });

    Ok((cluster_info, receivers))
}

#[allow(clippy::too_many_arguments)]
async fn process_grpc_update(
    update: SubscribeUpdate,
    config: &ClusterConfig,
    recent_blockhash: &Arc<RwLock<BlockHashUpdate>>,
    current_block_height: &Arc<AtomicU64>,
    current_slot: &Arc<RwLock<SlotUpdate>>,
    accounts: &Arc<DashMap<Pubkey, Account>>,
    slot_notifier: &Option<Sender<SlotUpdate>>,
    status_notifier: &Option<Sender<TransactionStatus>>,
    tx_notifier: &Option<Sender<ClusterTx>>,
) -> anyhow::Result<()> {
    let Some(update_oneof) = update.update_oneof else {
        return Ok(());
    };

    match update_oneof {
        UpdateOneof::Slot(update) if config.notify_slot || config.update_slot => {
            if update.status != config.commitment.commitment as i32 {
                return Ok(());
            }

            let slot_update = SlotUpdate::new(update.slot);

            if config.update_slot {
                *current_slot.write().await = slot_update.clone();
                trace!("updated slot: {}", update.slot);
            }

            if config.notify_slot {
                if let Some(slot_notifier) = slot_notifier {
                    slot_notifier
                        .send(slot_update)
                        .context("failed sending slot notification")?;
                    trace!("notified slot: {}", update.slot);
                }
            }
        }
        UpdateOneof::Account(update) => {
            let update = update.account.context("received null account")?;

            let (pubkey, account) = map_account_from_update(update)?;
            accounts.insert(pubkey, account);
        }
        UpdateOneof::BlockMeta(meta) if config.update_blockhash || config.update_blockheight => {
            let block_height = meta.block_height.map(|bh| bh.block_height);

            if let Some(block_height) = block_height {
                if config.update_blockheight {
                    current_block_height.store(block_height, Ordering::Relaxed);
                }
            }

            let blockhash = Hash::from_str(&meta.blockhash)
                .context("failed to parse blockhash from grpc update")?;

            if config.update_blockhash {
                let last_valid_block_height = block_height
                    .unwrap_or_else(|| current_block_height.load(Ordering::Relaxed))
                    + MAX_PROCESSING_AGE as u64;
                *recent_blockhash.write().await =
                    BlockHashUpdate::new(blockhash, last_valid_block_height);
                trace!(
                    "Updated blockhash. hash: {}, last-valid: {}",
                    blockhash, last_valid_block_height
                );
            }
        }
        UpdateOneof::TransactionStatus(status) => {
            let Some(notifier) = status_notifier.as_ref() else {
                return Ok(());
            };

            let signature = signature_from_bytes(status.signature)?;
            let payload = status.err.map(|err| err.err);
            let error = map_transaction_error(payload)?;

            let status = TransactionStatus::new(signature, error, status.slot);
            trace!("Got confirmed transaction {} from Laserstream", signature);

            notifier
                .send(status)
                .context("failed to broadcast grpc transaction status")?;
        }
        UpdateOneof::Transaction(tx) => {
            let Some(notifier) = tx_notifier.as_ref() else {
                return Ok(());
            };

            notifier
                .send(ClusterTx::Grpc(tx))
                .context("failed to broadcast grpc transaction")?;
        }
        _ => {}
    }

    Ok(())
}

fn signature_from_bytes(bytes: Vec<u8>) -> anyhow::Result<Signature> {
    <[u8; 64]>::try_from(bytes)
        .ok()
        .map(Signature::from)
        .context("failed to parse signature")
}

fn pubkey_from_bytes(bytes: Vec<u8>) -> anyhow::Result<Pubkey> {
    <[u8; 32]>::try_from(bytes)
        .ok()
        .map(Pubkey::from)
        .context("failed to parse signature")
}

fn map_account_from_update(
    update: SubscribeUpdateAccountInfo,
) -> anyhow::Result<(Pubkey, Account)> {
    let pubkey = pubkey_from_bytes(update.pubkey)?;
    let owner = pubkey_from_bytes(update.owner)?;

    let account = Account {
        lamports: update.lamports,
        data: update.data,
        owner,
        executable: update.executable,
        rent_epoch: update.rent_epoch,
    };

    Ok((pubkey, account))
}

fn map_transaction_error(payload: Option<Vec<u8>>) -> anyhow::Result<Option<TransactionError>> {
    let Some(bytes) = payload else {
        return Ok(None);
    };
    let error = bincode::deserialize::<TransactionError>(&bytes)
        .context("failed to deserialize TransactionError from grpc payload")?;
    match error {
        TransactionError::AlreadyProcessed => {
            log::info!("AlreadyProcessed: {}", error);
            Ok(None)
        }
        err => Ok(Some(err)),
    }
}
