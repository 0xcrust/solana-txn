use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use log::{error, trace};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::request::MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::TransactionError;
use tokio::sync::mpsc::Sender;

use crate::{
    ConfirmationResult, ConfirmationStatus, PendingTransaction,
    defaults::{CHECK_CONFIRMATION_FREQUENCY_RPC, COMMITMENT},
};

#[allow(clippy::too_many_arguments)]
pub fn start_rpc_confirmation_task(
    rpc_client: Arc<RpcClient>,
    current_block_height: Arc<AtomicU64>,
    confirmation_queue: Arc<DashMap<Signature, PendingTransaction>>,
    confirmation_sender: Sender<ConfirmationResult>,
    commitment: Option<CommitmentConfig>,
    check_confirmation_frequency: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let commitment = commitment.unwrap_or(COMMITMENT);

    tokio::spawn(async move {
        let confirmation_queue = Arc::clone(&confirmation_queue);
        let current_block_height = Arc::clone(&current_block_height);
        let wait_confirmation =
            check_confirmation_frequency.unwrap_or(CHECK_CONFIRMATION_FREQUENCY_RPC);
        let mut last_block_height = current_block_height.load(Ordering::Relaxed);

        loop {
            let current_block_height = current_block_height.load(Ordering::Relaxed);

            if confirmation_queue.is_empty() {
                tokio::time::sleep(wait_confirmation).await;
                continue;
            }
            let transactions_to_verify = confirmation_queue
                .iter()
                .filter(|x| x.validate(current_block_height, last_block_height))
                .map(|x| *x.key())
                .collect::<Vec<_>>();

            for signatures in transactions_to_verify.chunks(MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS)
            {
                let statuses = match rpc_client.get_signature_statuses(signatures).await {
                    Ok(statuses) => statuses.value,
                    Err(e) => {
                        error!("Error getting signature statuses for confirmation: {}", e);
                        continue;
                    }
                };

                for (signature, status) in signatures.iter().zip(statuses.into_iter()) {
                    if let Some((status, data)) = status
                        .filter(|status| status.satisfies_commitment(commitment))
                        .and_then(|status| {
                            confirmation_queue
                                .remove(signature)
                                .map(|(_, data)| (status, data))
                        })
                    {
                        trace!("Confirmed transaction with signature: {}", signature);
                        let error = match status.err {
                            Some(TransactionError::AlreadyProcessed) | None => None,
                            Some(error) => Some(error),
                        };

                        let response = ConfirmationResult {
                            id: data.id,
                            uuid: signature.to_string(),
                            status: ConfirmationStatus::Confirmed {
                                slot: status.slot,
                                error,
                            },
                            bundle_tracking_only: data.bundle_tracking_only,
                        };

                        if let Err(e) = confirmation_sender.send(response).await {
                            error!(
                                "Failed sending confirmed tx {} over channel: {}",
                                signature, e
                            );
                        }
                    };
                }
            }

            let expired_txns = confirmation_queue
                .iter()
                .filter(|x| !x.validate_strict(current_block_height))
                .map(|x| *x.key())
                .collect::<Vec<_>>();

            for signature in expired_txns {
                trace!(
                    "Failed to confirm transaction with signature: {}",
                    signature
                );
                let Some((_, data)) = confirmation_queue.remove(&signature) else {
                    unreachable!("attempted duplicate removal of signature {}", signature);
                };

                let response = ConfirmationResult {
                    id: data.id,
                    uuid: signature.to_string(),
                    status: ConfirmationStatus::Expired,
                    bundle_tracking_only: data.bundle_tracking_only,
                };
                if let Err(e) = confirmation_sender.send(response).await {
                    error!(
                        "Failed sending unconfirmed tx {} over channel: {}",
                        signature, e
                    );
                }
            }

            last_block_height = current_block_height;
            tokio::time::sleep(wait_confirmation).await;
        }

        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    })
}
