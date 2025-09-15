use crate::{
    ClusterConfig, ClusterEndpoints, ClusterReceivers, EndpointConfig, JitoTips, SlotUpdate,
    bootstrap::RpcData,
    defaults::{RPC_URL, SLOT_STREAMER_CAPACITY},
    rpc::polling::{
        start_accounts_polling_task, start_blockhash_polling_task, start_blockheight_polling_task,
        start_slot_polling_task,
    },
};
use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use anyhow::bail;
use dashmap::DashMap;
use solana_client::nonblocking::rpc_client::RpcClient;
use tokio::sync::{RwLock, broadcast};

pub(crate) fn start_rpc_task(
    endpoint_config: EndpointConfig,
    cluster_config: ClusterConfig,
    bootstrap: RpcData,
) -> anyhow::Result<(ClusterEndpoints, ClusterReceivers)> {
    let rpc_url = endpoint_config.rpc_url.unwrap_or(RPC_URL.to_string());
    let rpc = Arc::new(RpcClient::new(rpc_url));

    let ClusterConfig {
        commitment,
        notify_slot,
        notify_txn,
        notify_txn_status,
        update_accounts,
        update_blockhash,
        update_blockheight,
        update_slot,
    } = cluster_config;

    let recent_blockhash = Arc::new(RwLock::new(bootstrap.current_blockhash));
    let current_block_height = Arc::new(AtomicU64::new(bootstrap.current_block_height));
    let current_slot = Arc::new(RwLock::new(bootstrap.current_slot));
    let accounts = Arc::new(DashMap::new());

    let (slot_tx, slot_rx) = if notify_slot {
        let (tx, rx) = broadcast::channel::<SlotUpdate>(SLOT_STREAMER_CAPACITY);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    if notify_txn_status {
        bail!("transaction status streaming not supported over RPC");
    }

    if notify_txn {
        bail!("transaction streaming not supported over RPC");
    }

    if update_slot || notify_slot {
        start_slot_polling_task(
            rpc.clone(),
            current_slot.clone(),
            commitment,
            slot_tx,
            endpoint_config
                .poll_slot_frequency_ms
                .map(Duration::from_millis),
        );
    }

    if update_accounts && !endpoint_config.update_accounts.is_empty() {
        start_accounts_polling_task(
            rpc.clone(),
            accounts.clone(),
            endpoint_config.update_accounts.clone(),
            commitment,
            endpoint_config
                .poll_accounts_frequency_ms
                .map(Duration::from_millis),
        );
    }

    if update_blockhash {
        start_blockhash_polling_task(
            rpc.clone(),
            recent_blockhash.clone(),
            commitment,
            endpoint_config
                .poll_blockhash_frequency_ms
                .map(Duration::from_millis),
        );
    }

    if update_blockheight {
        start_blockheight_polling_task(
            rpc.clone(),
            current_block_height.clone(),
            commitment,
            endpoint_config
                .poll_block_height_frequency_ms
                .map(Duration::from_millis),
        );
    }

    let cluster_info = ClusterEndpoints {
        rpc,
        recent_blockhash,
        current_block_height,
        current_slot,
        accounts,
        jito_tips: Arc::new(RwLock::new(JitoTips::default())),
    };

    let receivers = ClusterReceivers {
        slot_streamer: slot_rx,
        tx_streamer: None,
        tx_status_streamer: None,
    };

    Ok((cluster_info, receivers))
}
