use agave_reserved_account_keys::ReservedAccountKeys;
use solana_bincode::limited_deserialize;
use solana_program::message::v0::{LoadedAddresses, LoadedMessage};
use solana_sdk::bs58;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::{AccountMeta, CompiledInstruction, Instruction};
use solana_sdk::message::{Message, SanitizedMessage, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::system_instruction::SystemInstruction;
use solana_sdk_ids::system_program;
use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::{
    UiInnerInstructions, UiInstruction, UiLoadedAddresses, UiTransactionStatusMeta,
};

use std::collections::HashMap;
use std::str::FromStr;

/// Transaction meta extensions
pub trait TransactionMeta {
    /// Get inner instructions
    fn get_inner_instructions(&self) -> Option<HashMap<u8, Vec<CompiledInstruction>>>;
    /// Get loaded addresses
    fn get_loaded_addresses(&self) -> Option<LoadedAddresses>;
    /// Get the cus consumed
    fn get_cus_consumed(&self) -> Option<u64>;
}

/// In-place cu-price and tip modifications.
pub trait ComputeMods {
    fn update_cu(&mut self, cu_price: u64) -> bool;
    fn update_tip(&mut self, tip: u64, targets: &[Pubkey]) -> bool;
}

impl ComputeMods for VersionedMessage {
    fn update_cu(&mut self, cu_price: u64) -> bool {
        match self {
            VersionedMessage::Legacy(message) => message.update_cu(cu_price),
            VersionedMessage::V0(message) => message.update_cu(cu_price),
        }
    }
    fn update_tip(&mut self, tip: u64, targets: &[Pubkey]) -> bool {
        match self {
            VersionedMessage::Legacy(message) => message.update_tip(tip, targets),
            VersionedMessage::V0(message) => message.update_tip(tip, targets),
        }
    }
}

impl ComputeMods for solana_sdk::message::legacy::Message {
    fn update_cu(&mut self, updated_cu_price: u64) -> bool {
        modify_cu_price_legacy(self, updated_cu_price)
    }
    fn update_tip(&mut self, tip: u64, targets: &[Pubkey]) -> bool {
        modify_transfer_amount_legacy(self, tip, targets)
    }
}

impl ComputeMods for solana_sdk::message::v0::Message {
    fn update_cu(&mut self, cu_price: u64) -> bool {
        modify_cu_price_v0(self, cu_price)
    }
    fn update_tip(&mut self, tip: u64, targets: &[Pubkey]) -> bool {
        modify_transfer_amount_v0(self, tip, targets)
    }
}

impl TransactionMeta for UiTransactionStatusMeta {
    fn get_inner_instructions(&self) -> Option<HashMap<u8, Vec<CompiledInstruction>>> {
        let inner_instructions: Option<&Vec<UiInnerInstructions>> =
            self.inner_instructions.as_ref().into();
        Some(HashMap::from_iter(inner_instructions?.iter().map(
            |inner_ix| {
                (
                    inner_ix.index,
                    inner_ix
                        .instructions
                        .iter()
                        .filter_map(|ix| match ix {
                            UiInstruction::Compiled(ix) => Some(CompiledInstruction {
                                program_id_index: ix.program_id_index,
                                accounts: ix.accounts.clone(),
                                data: bs58::decode(&ix.data).into_vec().unwrap(),
                            }),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                )
            },
        )))
    }

    fn get_loaded_addresses(&self) -> Option<LoadedAddresses> {
        let addresses: Option<&UiLoadedAddresses> = self.loaded_addresses.as_ref().into();
        let ui_loaded_addresses = addresses?;
        Some(LoadedAddresses {
            readonly: ui_loaded_addresses
                .readonly
                .iter()
                .map(|s| Pubkey::from_str(s.as_str()).unwrap())
                .collect(),
            writable: ui_loaded_addresses
                .writable
                .iter()
                .map(|s| Pubkey::from_str(s.as_str()).unwrap())
                .collect(),
        })
    }

    fn get_cus_consumed(&self) -> Option<u64> {
        match self.compute_units_consumed {
            OptionSerializer::Some(cus) => Some(cus),
            _ => None,
        }
    }
}

/// Decompile a [VersionedMessage] back into its instructions.
pub fn extract_instructions_from_versioned_message(
    message: &VersionedMessage,
    loaded_addresses: &LoadedAddresses,
) -> Vec<Instruction> {
    match &message {
        VersionedMessage::Legacy(message) => extract_instructions_from_legacy_message(message),
        VersionedMessage::V0(message) => {
            let reserved_account_keys = ReservedAccountKeys::default().active;
            let loaded_message =
                LoadedMessage::new_borrowed(message, loaded_addresses, &reserved_account_keys);
            let addrs: Vec<Pubkey> = loaded_message.account_keys().iter().copied().collect();
            message
                .instructions
                .iter()
                .map(|ix| {
                    let mut account_metas = vec![];
                    for idx in &ix.accounts {
                        let idx = *idx as usize;
                        let is_signer = loaded_message.is_signer(idx);
                        if loaded_message.is_writable(idx) {
                            account_metas
                                .push(AccountMeta::new(*addrs.get(idx).unwrap(), is_signer));
                        } else {
                            account_metas.push(AccountMeta::new_readonly(
                                *addrs.get(idx).unwrap(),
                                is_signer,
                            ));
                        }
                    }
                    let program = addrs.get(ix.program_id_index as usize).unwrap();
                    Instruction::new_with_bytes(*program, &ix.data, account_metas)
                })
                .collect()
        }
    }
}

/// Decompile a [Message] back into its instructions.
pub fn extract_instructions_from_legacy_message(message: &Message) -> Vec<Instruction> {
    let reserved_account_keys = ReservedAccountKeys::default().active;
    let message =
        match SanitizedMessage::try_from_legacy_message(message.clone(), &reserved_account_keys) {
            Ok(m) => m,
            Err(e) => {
                panic!(
                    "Failed to sanitize message due to {e}: {:#?} ({} keys) {:#?}, {:#?}",
                    message.header,
                    message.account_keys.len(),
                    message.instructions,
                    message.account_keys,
                );
            }
        };
    message
        .decompile_instructions()
        .iter()
        .map(|ix| {
            Instruction::new_with_bytes(
                *ix.program_id,
                ix.data,
                ix.accounts
                    .iter()
                    .map(|act| AccountMeta {
                        pubkey: *act.pubkey,
                        is_signer: act.is_signer,
                        is_writable: act.is_writable,
                    })
                    .collect(),
            )
        })
        .collect()
}

/// Modify the cu-price of a v0 message in-place
fn modify_cu_price_v0(message: &mut solana_sdk::message::v0::Message, cu_price: u64) -> bool {
    let account_keys = &message.account_keys;
    for instruction in message.instructions.iter_mut() {
        if try_update_cu_price(instruction, account_keys, cu_price) {
            return true;
        }
    }
    false
}

/// Modify the compute-unit price of a Legacy message in-place
fn modify_cu_price_legacy(
    message: &mut solana_sdk::message::legacy::Message,
    cu_price: u64,
) -> bool {
    let account_keys = &message.account_keys;
    for instruction in message.instructions.iter_mut() {
        if try_update_cu_price(instruction, account_keys, cu_price) {
            return true;
        }
    }

    false
}

/// Modify the amount of the first transfer found from an address in target
fn modify_transfer_amount_v0(
    message: &mut solana_sdk::message::v0::Message,
    lamports: u64,
    targets: &[Pubkey],
) -> bool {
    let account_keys = &message.account_keys;
    for instruction in message.instructions.iter_mut() {
        if try_update_transfer_amount(instruction, account_keys, targets, lamports) {
            return true;
        }
    }
    false
}

/// Modify the amount of the first transfer found from an address in target
fn modify_transfer_amount_legacy(
    message: &mut solana_sdk::message::legacy::Message,
    lamports: u64,
    targets: &[Pubkey],
) -> bool {
    let account_keys = &message.account_keys;
    for instruction in message.instructions.iter_mut() {
        if try_update_transfer_amount(instruction, account_keys, targets, lamports) {
            return true;
        }
    }
    false
}

fn try_update_cu_price(
    instruction: &mut CompiledInstruction,
    account_keys: &[Pubkey],
    updated: u64,
) -> bool {
    let program_id = account_keys[instruction.program_id_index as usize];
    if solana_sdk::compute_budget::check_id(&program_id) {
        let res = solana_sdk::borsh1::try_from_slice_unchecked(&instruction.data);
        if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(_micro_lamports)) = res {
            let Ok(updated_data) =
                borshv1::to_vec(&ComputeBudgetInstruction::SetComputeUnitPrice(updated))
            else {
                return false;
            };
            instruction.data = updated_data;
            return true;
        }
    }

    false
}

fn try_update_transfer_amount(
    instruction: &mut CompiledInstruction,
    account_keys: &[Pubkey],
    targets: &[Pubkey],
    updated: u64,
) -> bool {
    let program_id = account_keys[instruction.program_id_index as usize];
    let limit = std::mem::size_of::<SystemInstruction>() as u64;
    if program_id == system_program::ID {
        if let Ok(SystemInstruction::Transfer { lamports: _ }) =
            limited_deserialize(&instruction.data, limit)
        {
            let Some(account) = account_keys.get(instruction.accounts[1] as usize) else {
                return false;
            };
            if !targets.contains(account) {
                return false;
            }
            let transfer = SystemInstruction::Transfer { lamports: updated };
            let Ok(updated_data) = bincode::serialize(&transfer) else {
                return false;
            };
            instruction.data = updated_data;
            return true;
        }
    }

    false
}
