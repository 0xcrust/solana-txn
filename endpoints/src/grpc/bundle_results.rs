use crate::{PendingTransaction, metrics, send_txn::service::JitoGrpcConnection};

use std::sync::Arc;
use std::time::Duration;

use ::metrics::counter;
use dashmap::DashMap;
use either::Either;
use futures::StreamExt;
use jito_protos::bundle::{bundle_result, rejected::Reason};
use jito_searcher_client::setup_bundle_results_stream;
use log::{error, info, warn};
use tokio::sync::RwLock;

pub fn start_grpc_bundle_results_task(
    confirmation_queue: Arc<DashMap<String, PendingTransaction>>,
    searcher_client: Arc<RwLock<JitoGrpcConnection>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let confirmation_queue = Arc::clone(&confirmation_queue);

        let mut subscribed = 0;
        loop {
            if subscribed != 0 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            subscribed += 1;
            info!("subscribing to bundle results..subscribed={}", subscribed);

            let stream = match searcher_client.write().await.as_mut() {
                Either::Left(client) => setup_bundle_results_stream(client).await,
                Either::Right(client) => setup_bundle_results_stream(client).await,
            };

            let mut stream = match stream {
                Ok(stream) => stream,
                Err(e) => {
                    error!("Error subscribing to bundle results: {}", e);
                    continue;
                }
            };

            while let Some(item) = stream.next().await {
                let result = match item {
                    Ok(res) => res,
                    Err(e) => {
                        error!("Got error for bundle result: uuid: {}", e);
                        continue;
                    }
                };

                let source = match confirmation_queue.get(&result.bundle_id) {
                    None => {
                        warn!(
                            "got bundle result for untracked bundle-id: {}",
                            result.bundle_id
                        );
                        continue;
                    }
                    Some(val) => val.source.inner().to_string(),
                };

                let Some(res) = &result.result else {
                    error!("Received null result from jito bundle stream");
                    continue;
                };

                match res {
                    bundle_result::Result::Accepted(_accepted) => {
                        counter!(metrics::TOTAL_ACCEPTED_BUNDLES, "source" => source.clone())
                            .increment(1);
                    }
                    bundle_result::Result::Rejected(rejected) => match rejected.reason {
                        Some(Reason::DroppedBundle(_)) => {
                            counter!(metrics::DROPPED_BUNDLES, "source" => source.clone())
                                .increment(1);
                        }
                        Some(Reason::InternalError(_)) => {
                            counter!(metrics::INTERNAL_ERROR_BUNDLES, "source" => source.clone())
                                .increment(1);
                        }
                        Some(Reason::SimulationFailure(_)) => {
                            counter!(metrics::SIMULATION_FAILURE_BUNDLES, "source" => source.clone()).increment(1);
                        }
                        Some(Reason::StateAuctionBidRejected(_)) => {
                            counter!(metrics::STATE_AUCTION_BID_REJECTED_BUNDLES, "source" => source.clone()).increment(1);
                        }
                        Some(Reason::WinningBatchBidRejected(_)) => {
                            counter!(metrics::WINNING_BATCH_BID_REJECTED_BUNDLES, "source" => source.clone()).increment(1);
                        }
                        None => {}
                    },
                    _ => {}
                }
            }
        }

        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    })
}
