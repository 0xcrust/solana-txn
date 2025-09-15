use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use log::{error, trace};
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::mpsc::Sender;

use crate::jito_rpc::JitoClient;
use crate::{
    ConfirmationResult, ConfirmationStatus, PendingTransaction,
    defaults::{CHECK_CONFIRMATION_FREQUENCY_RPC, COMMITMENT},
};

#[allow(clippy::too_many_arguments)]
pub fn start_rpc_bundle_confirmation_task(
    rpc_client: Arc<JitoClient>,
    current_block_height: Arc<AtomicU64>,
    confirmation_queue: Arc<DashMap<String, PendingTransaction>>,
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
            let bundles_to_verify = confirmation_queue
                .iter()
                .filter(|x| x.validate(current_block_height, last_block_height))
                .map(|x| x.key().clone())
                .collect::<Vec<_>>();

            for bundle_ids in
                bundles_to_verify.chunks(JitoClient::MAX_GET_BUNDLE_STATUSES_QUERY_ITEMS)
            {
                let statuses = match rpc_client.get_bundle_statuses(bundle_ids, None).await {
                    Ok(res) => res.value,
                    Err(e) => {
                        error!("Error in jito getBundleStatuses request: {}", e);
                        continue;
                    }
                };

                for (bundle_id, status) in bundle_ids.iter().zip(statuses.into_iter()) {
                    if let Some((status, data)) = status
                        .satisfies_commitment(commitment)
                        .then_some(status)
                        .and_then(|status| {
                            confirmation_queue
                                .remove(bundle_id)
                                .map(|(_, data)| (status, data))
                        })
                    {
                        trace!("Confirmed bundle with id: {}", bundle_id);
                        let error = None; // todo: assumed that bundles cannot error

                        let response = ConfirmationResult {
                            id: data.id,
                            uuid: bundle_id.to_string(),
                            status: ConfirmationStatus::Confirmed {
                                slot: status.slot,
                                error,
                            },
                            bundle_tracking_only: data.bundle_tracking_only,
                        };

                        if let Err(e) = confirmation_sender.send(response).await {
                            error!(
                                "Failed sending confirmed bundle {} over channel: {}",
                                bundle_id, e
                            );
                        }
                    };
                }
            }

            let expired_bundles = confirmation_queue
                .iter()
                .filter(|x| !x.validate_strict(current_block_height))
                .map(|x| x.key().clone())
                .collect::<Vec<_>>();

            for id in expired_bundles {
                trace!("Failed to confirm bundle with id: {}", id);
                let Some((_, data)) = confirmation_queue.remove(&id) else {
                    unreachable!("attempted duplicate removal of bundle id {}", id);
                };

                let response = ConfirmationResult {
                    id: data.id,
                    uuid: id.clone(),
                    status: ConfirmationStatus::Expired,
                    bundle_tracking_only: data.bundle_tracking_only,
                };
                if let Err(e) = confirmation_sender.send(response).await {
                    error!(
                        "Failed sending unconfirmed bundle {} over channel: {}",
                        id, e
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
