use crate::{
    SlotUpdate,
    defaults::{AVERAGE_SLOT_CHANGE_TIME, POLL_SLOT_FREQUENCY},
};
use std::time::Duration;
use std::{sync::Arc, time::Instant};

use log::{error, trace};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use tokio::sync::RwLock;
use tokio::time::interval;

pub fn start_slot_polling_task(
    rpc_client: Arc<RpcClient>,
    current_slot: Arc<RwLock<SlotUpdate>>,
    commitment: CommitmentConfig,
    slot_notifier: Option<tokio::sync::broadcast::Sender<SlotUpdate>>,
    duration: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        log::info!("Spawning slot polling task..");

        let mut poll_interval = interval(duration.unwrap_or(POLL_SLOT_FREQUENCY));
        let mut force_update_interval = interval(AVERAGE_SLOT_CHANGE_TIME);

        let mut last_polled_slot = current_slot.read().await.slot;
        let mut last_polled_at = Instant::now();

        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    for i in 1..=3 {
                        match rpc_client.get_slot_with_commitment(commitment).await {
                            Ok(recent_slot) => {
                                let slot = SlotUpdate::new(recent_slot);

                                *current_slot.write().await = slot.clone();
                                last_polled_slot = slot.slot;
                                last_polled_at = Instant::now();
                                trace!("Refreshed slot by RPC. slot: {}", slot.slot);

                                break;
                            }
                            Err(e) => log::error!("Failed getSlot request({i}/5): {e}"),
                        }
                    }
                }

                _ = force_update_interval.tick() => {
                    if force_update_interval.period() > poll_interval.period() {
                        continue;
                    }

                    let slot_diff = get_slot_diff(last_polled_at.elapsed().as_millis());
                    let slot_update = SlotUpdate::new(last_polled_slot + slot_diff);

                    *current_slot.write().await = slot_update.clone();

                    if let Some(ref slot_notifier) = slot_notifier {
                        if let Err(e) = slot_notifier.send(slot_update) {
                            error!("failed sending slot notification: {}", e);
                        } else {
                            trace!("sent slot notification");
                        }
                    }
                }
            }
        }

        #[allow(unreachable_code)]
        Ok(())
    })
}

pub fn get_slot_diff(millis_elapsed: u128) -> u64 {
    let slots = millis_elapsed / AVERAGE_SLOT_CHANGE_TIME.as_millis();
    log::trace!("seconds: {}, slots: {}", millis_elapsed, slots);
    slots.try_into().unwrap_or_default()
}
