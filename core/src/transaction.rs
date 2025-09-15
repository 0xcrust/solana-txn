use super::tx_ext::TransactionMeta;
use agave_reserved_account_keys::ReservedAccountKeys;
use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{Context, anyhow, bail};
use solana_bincode::limited_deserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::address_lookup_table::state::AddressLookupTable;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::v0::{LoadedAddresses, MessageAddressTableLookup};
use solana_sdk::message::{AccountKeys, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::slot_history::Slot;
use solana_sdk::vote::instruction::VoteInstruction;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta,
    UiTransactionStatusMeta,
};
use yellowstone_grpc_proto::convert_from::{create_tx_meta, create_tx_versioned};
use yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction;

#[derive(Debug, Clone)]
pub struct TxWithMetadata {
    /// The transaction signatures
    pub signatures: Vec<Signature>,

    /// The transaction message
    pub message: VersionedMessage,

    /// The slot corresponding to this transaction's entry
    pub slot: Slot,

    /// Whether or not this transaction is a vote transaction
    pub is_vote: bool,

    /// Block timestamp
    pub block_time: Option<i64>,

    /// Loaded addresses for this transaction
    pub loaded_addresses: Option<LoadedAddresses>,

    /// The transaction meta
    pub meta: UiTransactionStatusMeta,

    /// The blockhash used in sending the transaction
    pub recent_blockhash: Hash,
}

impl TxWithMetadata {
    pub fn is_legacy(&self) -> bool {
        matches!(self.message, VersionedMessage::Legacy(_))
    }

    pub fn is_versioned(&self) -> bool {
        matches!(self.message, VersionedMessage::V0(_))
    }

    pub fn account_keys(&self) -> AccountKeys<'_> {
        AccountKeys::new(
            self.message.static_account_keys(),
            self.loaded_addresses.as_ref(),
        )
    }

    pub fn address_table_lookups(&self) -> Option<Vec<MessageAddressTableLookup>> {
        self.message.address_table_lookups().map(|x| x.to_vec())
    }

    pub fn read_write_accounts(&self) -> ReadWriteAccounts {
        let mut readable = vec![];
        let mut writable = vec![];
        let keys = self.message.static_account_keys();

        for (index, key) in keys.iter().enumerate() {
            if self
                .message
                .is_maybe_writable(index, Some(&ReservedAccountKeys::default().active))
            {
                writable.push(*key);
            } else {
                readable.push(*key);
            }
        }

        ReadWriteAccounts { writable, readable }
    }

    pub fn signature(&self) -> Signature {
        self.signatures[0]
    }

    pub fn signatures(&self) -> &[Signature] {
        let signatures = self.message.header().num_required_signatures as usize;
        &self.signatures[0..signatures]
    }

    pub fn signers(&self) -> &[Pubkey] {
        let signatures = self.message.header().num_required_signatures as usize;
        &self.message.static_account_keys()[0..signatures]
    }
}

pub struct ReadWriteAccounts {
    pub writable: Vec<Pubkey>,
    pub readable: Vec<Pubkey>,
}

pub fn unified_tx_from_grpc(
    mut update: SubscribeUpdateTransaction,
) -> anyhow::Result<TxWithMetadata> {
    let slot = update.slot;
    let Some(mut transaction_info) = update.transaction.take() else {
        bail!("Missing transaction data in Geyser update");
    };
    let Some(meta) = transaction_info.meta.take() else {
        bail!("Missing transaction status meta in Geyser update");
    };

    let meta = UiTransactionStatusMeta::from(create_tx_meta(meta).map_err(|e| anyhow!(e))?);
    let Some(transaction) = transaction_info.transaction.take() else {
        bail!("Missing transaction data in Geyser update");
    };
    let versioned_tx = create_tx_versioned(transaction).map_err(|e| anyhow!(e))?;
    // let _signature = Signature::try_from(transaction_info.signature)
    //     .map_err(|_| anyhow!("Failed converting signature from u8 array"))?;
    let recent_blockhash = *versioned_tx.message.recent_blockhash();

    let loaded_addresses = match meta.loaded_addresses.as_ref() {
        OptionSerializer::Some(meta) => Some(LoadedAddresses {
            writable: meta
                .writable
                .iter()
                .filter_map(|key_str| Pubkey::from_str(key_str).ok())
                .collect(),
            readonly: meta
                .readonly
                .iter()
                .filter_map(|key_str| Pubkey::from_str(key_str).ok())
                .collect(),
        }),
        _ => None,
    };

    Ok(TxWithMetadata {
        signatures: versioned_tx.signatures,
        slot,
        is_vote: transaction_info.is_vote,
        message: versioned_tx.message,
        loaded_addresses,
        meta,
        recent_blockhash,
        block_time: Some(0),
    })
}

pub fn unified_tx_from_rpc(
    rpc_tx: EncodedTransactionWithStatusMeta,
    slot: u64,
    block_time: Option<i64>,
) -> anyhow::Result<TxWithMetadata> {
    let meta = rpc_tx
        .meta
        .context("Missing transaction status meta in RPC transaction")?;
    let versioned_tx = rpc_tx
        .transaction
        .decode()
        .context("failed to decode RPC transaction")?;
    let is_vote = versioned_tx.message.instructions().iter().any(|i| {
        i.program_id(versioned_tx.message.static_account_keys())
            .eq(&solana_sdk::vote::program::id())
            && limited_deserialize::<VoteInstruction>(
                &i.data,
                std::mem::size_of::<VoteInstruction>() as u64,
            )
            .map(|vi| vi.is_simple_vote())
            .unwrap_or(false)
    });
    let recent_blockhash = *versioned_tx.message.recent_blockhash();

    let loaded_addresses = match meta.loaded_addresses.as_ref() {
        OptionSerializer::Some(meta) => Some(LoadedAddresses {
            writable: meta
                .writable
                .iter()
                .filter_map(|key_str| Pubkey::from_str(key_str).ok())
                .collect(),
            readonly: meta
                .readonly
                .iter()
                .filter_map(|key_str| Pubkey::from_str(key_str).ok())
                .collect(),
        }),
        _ => None,
    };

    Ok(TxWithMetadata {
        signatures: versioned_tx.signatures,
        slot,
        message: versioned_tx.message,
        meta,
        loaded_addresses,
        recent_blockhash,
        is_vote,
        block_time,
    })
}

impl TryFrom<EncodedConfirmedTransactionWithStatusMeta> for TxWithMetadata {
    type Error = anyhow::Error;

    fn try_from(value: EncodedConfirmedTransactionWithStatusMeta) -> Result<Self, Self::Error> {
        unified_tx_from_rpc(value.transaction, value.slot, value.block_time)
    }
}

impl TryFrom<SubscribeUpdateTransaction> for TxWithMetadata {
    type Error = anyhow::Error;

    fn try_from(value: SubscribeUpdateTransaction) -> Result<Self, Self::Error> {
        unified_tx_from_grpc(value)
    }
}

impl TransactionMeta for TxWithMetadata {
    fn get_cus_consumed(&self) -> Option<u64> {
        self.meta.get_cus_consumed()
    }

    fn get_inner_instructions(&self) -> Option<HashMap<u8, Vec<CompiledInstruction>>> {
        self.meta.get_inner_instructions()
    }

    fn get_loaded_addresses(&self) -> Option<LoadedAddresses> {
        self.meta.get_loaded_addresses()
    }
}

pub async fn get_loaded_addresses_from_rpc(
    client: &RpcClient,
    lookups: &[MessageAddressTableLookup],
) -> Result<LoadedAddresses, anyhow::Error> {
    let mut loaded_addresses = vec![];
    let keys = lookups.iter().map(|t| t.account_key).collect::<Vec<_>>();
    let lookup_accounts = client.get_multiple_accounts(&keys).await?;
    debug_assert!(lookup_accounts.len() == lookups.len());
    for (i, account) in lookup_accounts.into_iter().enumerate() {
        if account.is_none() {
            return Err(anyhow!("Failed to get account for address table lookup"));
        }
        let account = account.unwrap();
        let lookup_table = AddressLookupTable::deserialize(&account.data)
            .map_err(|_| anyhow!("failed to deserialize account lookup table"))?;
        loaded_addresses.push(LoadedAddresses {
            writable: lookups[i]
                .writable_indexes
                .iter()
                .map(|idx| {
                    lookup_table
                        .addresses
                        .get(*idx as usize)
                        .copied()
                        .ok_or(anyhow!(
                            "account lookup went out of bounds of address lookup table"
                        ))
                })
                .collect::<Result<_, _>>()?,
            readonly: lookups[i]
                .readonly_indexes
                .iter()
                .map(|idx| {
                    lookup_table
                        .addresses
                        .get(*idx as usize)
                        .copied()
                        .ok_or(anyhow!(
                            "account lookup went out of bounds of address lookup table"
                        ))
                })
                .collect::<Result<_, _>>()?,
        });
    }
    Ok(LoadedAddresses::from_iter(loaded_addresses))
}
