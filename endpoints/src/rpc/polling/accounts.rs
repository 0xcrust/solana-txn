use crate::{defaults::POLL_ACCOUNTS_FREQUENCY, rpc::accounts::get_multiple_account_data};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use log::error;
use solana_account::Account;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;
use solana_sdk::commitment_config::CommitmentConfig;

pub fn start_accounts_polling_task(
    rpc_client: Arc<RpcClient>,
    accounts_map: Arc<DashMap<Pubkey, Account>>,
    addresses: Vec<Pubkey>,
    commitment_config: CommitmentConfig,
    duration: Option<Duration>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        log::info!("Spawning accounts polling task..");
        let duration = duration.unwrap_or(POLL_ACCOUNTS_FREQUENCY);

        loop {
            for i in 1..=3 {
                match get_multiple_account_data(&rpc_client, &addresses, Some(commitment_config))
                    .await
                {
                    Ok(accounts) => {
                        for (pubkey, account) in addresses.iter().zip(accounts.into_iter()) {
                            let Some(account) = account else {
                                continue;
                            };
                            accounts_map.insert(*pubkey, account);
                        }
                    }
                    Err(e) => error!("failed to refresh accounts({i}/3): {e}"),
                }
            }

            tokio::time::sleep(duration).await;
        }

        #[allow(unreachable_code)]
        Ok(())
    })
}
