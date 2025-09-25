use crate::{
    ConfirmationResult, ConfirmationStatus, PendingTransaction, TransactionStatus,
    defaults::CHECK_CONFIRMATION_FREQUENCY_GRPC,
};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use log::{error, trace};
use solana_sdk::signature::Signature;
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tokio::sync::mpsc::Sender;
use tokio::time::interval;

pub fn start_grpc_confirmation_task(
    current_block_height: Arc<AtomicU64>,
    confirmation_queue: Arc<DashMap<Signature, PendingTransaction>>,
    confirmation_sender: Sender<ConfirmationResult>,
    mut txn_streamer: Receiver<TransactionStatus>,
    check_confirmation_frequency: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let confirmation_queue = Arc::clone(&confirmation_queue);
        let current_block_height = Arc::clone(&current_block_height);
        let mut check_expiry =
            interval(check_confirmation_frequency.unwrap_or(CHECK_CONFIRMATION_FREQUENCY_GRPC));

        let mut last_block_height = current_block_height.load(Ordering::Relaxed);

        loop {
            tokio::select! {
                res = txn_streamer.recv() => {
                    let status = match res {
                        Ok(status) => status,
                        Err(RecvError::Closed) => {
                            error!("txn streamer channel closed");
                            continue
                        }
                        _ => continue
                    };
                    let signature = status.signature;

                    let Some((_, data)) = confirmation_queue.remove(&signature) else {
                        continue;
                    };

                    trace!("grpc: confirmed tx {} from grpc tx-statuses queue", signature);
                    let response = ConfirmationResult {
                        id: data.id,
                        uuid: signature.to_string(),
                        status: ConfirmationStatus::Confirmed {
                            slot: status.slot,
                            error: status.error
                        },
                        bundle_tracking_only: data.bundle_tracking_only
                    };

                    if let Err(e) = confirmation_sender.send(response).await {
                        error!(
                            "Failed sending confirmed tx {} over channel: {}",
                            signature, e
                        );
                    }
                }
                _ = check_expiry.tick() => {
                    let current_block_height = current_block_height.load(Ordering::Relaxed);
                    let expired_txns = confirmation_queue
                        .iter()
                        .filter(|x| !x.validate(current_block_height, last_block_height))
                        .map(|x| *x.key())
                        .collect::<Vec<_>>();

                    for signature in expired_txns {
                        trace!(
                            "grpc: Failed to confirm transaction with signature: {}",
                            signature
                        );
                        let Some((_, data)) = confirmation_queue.remove(&signature) else {
                            unreachable!("attempted duplicate removal of signature {}", signature);
                        };

                        let response = ConfirmationResult {
                            id: data.id,
                            uuid: signature.to_string(),
                            status: ConfirmationStatus::Expired,
                            bundle_tracking_only: data.bundle_tracking_only
                        };
                        if let Err(e) = confirmation_sender.send(response).await {
                            error!(
                                "Failed sending unconfirmed tx {} over channel: {}",
                                signature, e
                            );
                        }
                    }
                    last_block_height = current_block_height;
                }
            }
        }

        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    })
}
