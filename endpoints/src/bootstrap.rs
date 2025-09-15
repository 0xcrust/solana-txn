use crate::rpc::accounts::get_multiple_account_data;
use crate::{BlockHashUpdate, SlotUpdate};
use std::collections::HashMap;
use std::time::Instant;

use anyhow::Context;
use solana_account::Account;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_pubkey::Pubkey;
use solana_sdk::commitment_config::CommitmentConfig;

#[derive(Clone)]
pub struct RpcData {
    pub current_blockhash: BlockHashUpdate,
    pub current_block_height: u64,
    pub current_slot: SlotUpdate,
    pub accounts_map: HashMap<Pubkey, Account>,
}

pub async fn bootstrap_endpoint_data(
    rpc_client: &RpcClient,
    addresses: &[Pubkey],
    attempts: u8,
) -> anyhow::Result<RpcData> {
    let mut current_slot = Option::<SlotUpdate>::None;
    let mut current_blockhash = Option::<BlockHashUpdate>::None;
    let mut current_block_height = Option::<u64>::None;
    let mut accounts_map = Option::<HashMap<Pubkey, Account>>::None;
    if addresses.is_empty() {
        accounts_map = Some(HashMap::new());
    }

    for i in 1..=attempts {
        if current_block_height.is_none() {
            match rpc_client
                .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
                .await
            {
                Ok((blockhash, last_valid_block_height)) => {
                    current_blockhash = Some(BlockHashUpdate {
                        blockhash,
                        last_valid_block_height,
                    })
                }
                Err(e) => log::error!("Failed fetching blockhash, attempt:{} : {}", i, e),
            }
        }

        if current_slot.is_none() {
            match rpc_client
                .get_slot_with_commitment(CommitmentConfig::processed())
                .await
            {
                Ok(slot) => {
                    current_slot = Some(SlotUpdate {
                        slot,
                        instant: Instant::now(),
                    })
                }
                Err(e) => log::error!("Failed fetching slot, attempts:{} : {}", i, e),
            }
        }

        if current_block_height.is_none() {
            match rpc_client.get_block_height().await {
                Ok(block_height) => current_block_height = Some(block_height),
                Err(e) => log::error!(
                    "Failed fetching current blockheight, attempts:{} : {}",
                    i,
                    e
                ),
            }
        }

        if accounts_map.is_none() {
            match get_multiple_account_data(
                rpc_client,
                addresses,
                Some(CommitmentConfig::confirmed()),
            )
            .await
            {
                Ok(data) => {
                    let mut map = HashMap::new();
                    for (address, data) in addresses.iter().zip(data) {
                        if let Some(data) = data {
                            map.insert(*address, data);
                        }
                    }
                    accounts_map = Some(map);
                }
                Err(e) => log::error!("Failed fetching accounts, attempts:{} : {}", i, e),
            }
        }

        if current_blockhash.is_some()
            && current_slot.is_some()
            && current_block_height.is_some()
            && accounts_map.is_some()
        {
            break;
        }
    }

    Ok(RpcData {
        current_blockhash: current_blockhash.context("failed to bootstrap blockhash data")?,
        current_block_height: current_block_height
            .context("failed to bootstrap current block_height")?,
        current_slot: current_slot.context("failed to bootstrap current slot")?,
        accounts_map: accounts_map.context("failed to bootstrap accounts")?,
    })
}
