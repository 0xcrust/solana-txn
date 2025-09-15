use crate::{BlockHashUpdate, defaults::POLL_BLOCKHASH_FREQUENCY};
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};
use std::time::Duration;

use log::{debug, error};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::RwLock;

pub fn start_blockhash_polling_task(
    rpc_client: Arc<RpcClient>,
    blockhash_notif: Arc<RwLock<BlockHashUpdate>>,
    commitment_config: CommitmentConfig,
    duration: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        log::info!("Spawning blockhash polling task..");
        let duration = duration.unwrap_or(POLL_BLOCKHASH_FREQUENCY);

        loop {
            for i in 1..=3 {
                match rpc_client
                    .get_latest_blockhash_with_commitment(commitment_config)
                    .await
                {
                    Ok((blockhash, last_valid_block_height)) => {
                        debug!(
                            "Got new blockhash: {}, {}",
                            blockhash, last_valid_block_height
                        );

                        let mut notif = blockhash_notif.write().await;
                        *notif = BlockHashUpdate::new(blockhash, last_valid_block_height);

                        break;
                    }
                    Err(e) => error!("failed to get latest blockhash({i}/5): {e}"),
                }
            }

            tokio::time::sleep(duration).await;
        }

        #[allow(unreachable_code)]
        Ok(())
    })
}

pub fn start_blockheight_polling_task(
    rpc_client: Arc<RpcClient>,
    current_blockheight: Arc<AtomicU64>,
    commitment_config: CommitmentConfig,
    duration: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        log::info!("Spawning block_height polling task..");
        let duration = duration.unwrap_or(POLL_BLOCKHASH_FREQUENCY);

        loop {
            for i in 1..=5 {
                match rpc_client
                    .get_block_height_with_commitment(commitment_config)
                    .await
                {
                    Ok(block_height) => {
                        debug!("Got new block_height: {}", block_height);
                        current_blockheight.store(block_height, Ordering::Relaxed);

                        break;
                    }
                    Err(e) => error!("failed to get latest block_height({i}/5): {e}"),
                }
            }

            tokio::time::sleep(duration).await;
        }

        #[allow(unreachable_code)]
        Ok(())
    })
}
