use crate::send_txn::service::{JitoConnection, TransactionService};
use crate::{
    ConfirmationResult, ConfirmationStatus, ExecutionResult, Processed, SingleOrBundled, metrics,
};

use ::metrics::{counter, gauge, histogram};
use anyhow::Context;
use log::{debug, error, trace, warn};
use tokio::sync::mpsc;

pub fn process_task(
    service: &TransactionService,
    mut tx_confirmation_receiver: mpsc::Receiver<ConfirmationResult>,
    mut bundle_confirmation_receiver: mpsc::Receiver<ConfirmationResult>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let service = service.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(data) = tx_confirmation_receiver.recv() => {
                    if let Err(e) = service.process_confirmation(data).await {
                        error!("send error: {}", e);
                    }
                }
                Some(data) = bundle_confirmation_receiver.recv() => {
                    if let Err(e) = service.process_confirmation(data).await {
                        error!("send error: {}", e);
                    }
                }
            }
        }

        #[allow(unreachable_code)]
        Ok(())
    })
}

impl TransactionService {
    pub async fn process_confirmation(&self, res: ConfirmationResult) -> anyhow::Result<()> {
        trace!(
            "Received confirmation response for {}. bundle-tracking-only: {}",
            res.id, res.bundle_tracking_only
        );

        let jito_transport = match self.jito {
            JitoConnection::Grpc {
                rpc_client: _,
                client: _,
            } => "grpc",
            JitoConnection::Rpc(_) => "rpc",
        };

        let (kind, source) = {
            let data = self
                .tx_queue
                .get(&res.id)
                .context("couldn't find tx data in queue")?;
            let kind = if data.is_bundled() {
                "bundled"
            } else {
                "single"
            };
            let source = data.source_id.inner().to_string();
            (kind, source)
        };

        if matches!(
            res.status,
            ConfirmationStatus::Confirmed { slot: _, error: _ }
        ) {
            counter!(metrics::TOTAL_LANDED_BUNDLES, "source" => source.clone(), "tx_kind" => kind, "jito_transport" => jito_transport).increment(1);
        };

        if res.bundle_tracking_only {
            return Ok(());
        }

        let (_, data) = self
            .tx_queue
            .remove(&res.id)
            .context("couldn't find tx data in queue")?;
        gauge!(metrics::TX_QUEUE_COUNT).set(self.tx_queue.len() as f64);
        let size = self
            .tx_queue
            .iter()
            .map(|x| std::mem::size_of_val(x.value()))
            .sum::<usize>();
        gauge!(metrics::TX_QUEUE_SIZE).set(size as f64);

        let handle = self
            .handles
            .get(&data.source_id)
            .context("handle not found for source")?;

        let signature = match &data.data {
            SingleOrBundled::Bundled(data) => data.uuid.clone(),
            SingleOrBundled::Single(data) => data.signature.to_string(),
        };

        match res.status {
            ConfirmationStatus::Confirmed { slot, error } => {
                let fee = data.fee();
                histogram!(metrics::LANDED_ATTEMPTS, "source" => source.clone(), "tx_kind" => kind)
                    .record(data.attempts);
                histogram!(metrics::LANDED_TIPS, "source" => source.clone(), "tx_kind" => kind)
                    .record(fee.tip as f64);
                histogram!(metrics::LANDED_PRIOFEE, "source" => source.clone(), "tx_kind" => kind)
                    .record(fee.priority as f64);
                if slot > data.send_slot {
                    let slot_delay = slot.saturating_sub(data.send_slot + 1);
                    histogram!(metrics::CONFIRMATION_DELAY, "source" => source.clone(), "tx_kind" => kind)
                        .record(slot_delay as f64);
                } else {
                    warn!("processed slot is less than or equal to send slot. metric not recorded");
                }
                counter!(metrics::UNIQUE_LANDED_TXNS, "source" => source.clone(), "tx_kind" => kind).increment(1);
                if error.is_none() {
                    counter!(metrics::UNIQUE_SUCCESS_TXNS, "source" => source.clone(), "tx_kind" => kind).increment(1);
                }

                match &data.data {
                    SingleOrBundled::Bundled(data) => {
                        debug!("https://explorer.jito.wtf/bundle/{}", data.uuid);
                    }
                    SingleOrBundled::Single(data) => {
                        debug!("confirmed txn: https://solscan.io/tx/{}", data.signature);
                    }
                };

                let result = ExecutionResult {
                    id: data.id,
                    processed: Some(Processed::new(slot)),
                    signature,
                    bundled: data.is_bundled(),
                    attempts: data.attempts,
                    tx_count: data.tx_count(),
                    error,
                    fee: data.fee(),
                };

                if let Err(e) = handle.value().result_sender.try_send(result) {
                    debug!("Failed to report confirmed result: {}", e);
                }
            }
            ConfirmationStatus::Expired => {
                if data.can_reschedule() {
                    self.tx_sender
                        .send(data)
                        .await
                        .context("failed to reschedule transaction")?;
                } else {
                    let result = ExecutionResult {
                        id: data.id,
                        processed: None,
                        signature,
                        bundled: data.is_bundled(),
                        attempts: data.attempts,
                        tx_count: data.tx_count(),
                        error: None,
                        fee: data.fee(),
                    };

                    if let Err(e) = handle.value().result_sender.try_send(result) {
                        debug!("Failed to report unconfirmed result: {}", e);
                    }
                }
            }
        }

        Ok(())
    }
}
