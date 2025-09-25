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

/// Add an instruction to the transaction without re-compilation
pub trait TransactionMods {
    fn insert_ix(&mut self, idx: usize, ix: Instruction);
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

impl TransactionMods for VersionedMessage {
    fn insert_ix(&mut self, idx: usize, ix: Instruction) {
        match self {
            VersionedMessage::Legacy(message) => message.insert_ix(idx, ix),
            VersionedMessage::V0(message) => message.insert_ix(idx, ix),
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

impl TransactionMods for solana_sdk::message::legacy::Message {
    fn insert_ix(&mut self, idx: usize, ix: Instruction) {
        insert_ix(&mut self.instructions, &mut self.account_keys, idx, ix);
    }
}

impl TransactionMods for solana_sdk::message::v0::Message {
    fn insert_ix(&mut self, idx: usize, ix: Instruction) {
        insert_ix(&mut self.instructions, &mut self.account_keys, idx, ix);
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

fn insert_ix(
    instructions: &mut Vec<CompiledInstruction>,
    account_keys: &mut Vec<Pubkey>,
    idx: usize,
    ix: Instruction,
) {
    let find_or_insert = |account_keys: &mut Vec<Pubkey>, target: Pubkey| -> u8 {
        let idx = account_keys
            .iter()
            .enumerate()
            .find_map(|(idx, key)| (*key == target).then_some(idx));
        match idx {
            Some(idx) => idx as u8,
            None => {
                let idx = account_keys.len();
                account_keys.push(target);
                idx as u8
            }
        }
    };

    let program_id_index = find_or_insert(account_keys, ix.program_id);
    let accounts = ix
        .accounts
        .into_iter()
        .map(|i| find_or_insert(account_keys, i.pubkey))
        .collect();

    let compiled_ix = CompiledInstruction {
        program_id_index,
        accounts,
        data: ix.data,
    };

    let insert_idx = std::cmp::min(idx, instructions.len());
    instructions.insert(insert_idx, compiled_ix);
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

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_config::RpcTransactionConfig;
    use solana_pubkey::{Pubkey, pubkey};
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::compute_budget::ComputeBudgetInstruction;
    use solana_sdk::signature::Signature;
    use solana_transaction_status::UiTransactionEncoding;

    use super::ComputeMods;
    use super::TransactionMods;
    use crate::priority::PriorityFee;
    use crate::transaction::TxWithMetadata;

    const FIXTURES: &[&str] = &[
        "2pstHqTYrrJnBfvj5pmd1jZLaNo11Dm1sEdYUVWPEPd7tJx5T98YaeJoFcvK2Q3DXUzti6unMBccE9zw8yB6NQsd",
        "5MVAmz9EA6xzpsJiMo9dTy1HTx33Rmq3796gdUK2uACokBu8dmQzrVp1HKMZuS8UL4JDiNmGFBXCxHuU46A1wxC1",
        "234tKjmeRnw6cNTskBC4CiLUg8AAEdkDCr88mQatkPwp5LA4NSsuCCFFqh4mTUUUyzKqvqKrHxFm3kr3vsEp5pyE",
        "3UuztBgJFZjVghYtDUKzUhw7s61umxdzXx4d6iVGMKfMNGjRNRBUe3Kr3qg9G2ND6mSHbrzhe2pvZrmpPBRiszr5",
        "32GVtufP4Lw3osHwK3wqru3NfUTjdPr2EohHfBrmcMVKfRFHRJrMheGmxRZ6poP6QofF8QXj4sdLT1oQVYFJGMUq",
        "3YMo98JnYHEhTJnrecGujGeYCgvuDt3Qwjy9GBhYZqWxxC4SGneSaRcAtsBq9b4udV9gLQ4h1xbzeBLfhXqwjvAa",
    ];

    const TIP_ACCOUNTS: [Pubkey; 8] = [
        pubkey!("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
        pubkey!("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe"),
        pubkey!("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY"),
        pubkey!("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49"),
        pubkey!("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh"),
        pubkey!("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt"),
        pubkey!("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL"),
        pubkey!("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT"),
    ];

    fn build_tip_ix(from: &Pubkey, value: u64) -> solana_sdk::instruction::Instruction {
        solana_sdk::system_instruction::transfer(from, pick_jito_recipient(), value)
    }

    fn pick_jito_recipient() -> &'static Pubkey {
        use rand::Rng;
        &TIP_ACCOUNTS[rand::rng().random_range(0..TIP_ACCOUNTS.len())]
    }

    #[tokio::test]
    async fn test_compute_mods() {
        let rpc_url = if dotenv::dotenv().is_ok() {
            std::env::var("RPC_URL").unwrap_or("https://api.mainnet-beta.solana.com".to_string())
        } else {
            "https://api.mainnet-beta.solana.com".to_string()
        };
        let client = RpcClient::new(rpc_url);

        for signature in FIXTURES {
            println!("fetching transaction for signature {}", signature);
            let transaction = client
                .get_transaction_with_config(
                    &Signature::from_str(signature).unwrap(),
                    RpcTransactionConfig {
                        encoding: Some(UiTransactionEncoding::Base64),
                        commitment: Some(CommitmentConfig::finalized()),
                        max_supported_transaction_version: Some(0),
                    },
                )
                .await
                .unwrap();

            let mut tx = TxWithMetadata::try_from(transaction).unwrap();
            let cu_price = 195647;
            let jito_tip = 185647;

            if !tx.message.update_cu(cu_price) {
                tx.message.insert_ix(
                    0,
                    ComputeBudgetInstruction::set_compute_unit_price(cu_price),
                );
            }
            if !tx.message.update_tip(jito_tip, &TIP_ACCOUNTS) {
                let from = tx.signers().first().unwrap();
                tx.message.insert_ix(0, build_tip_ix(&from, jito_tip));
            }
            // make sure message is still valid
            tx.message.sanitize().unwrap();
            let priority = PriorityFee::from_versioned_message(&tx.message, &TIP_ACCOUNTS);
            println!("priority: {:#?}", priority);
            assert_eq!(priority.tip, jito_tip);
            assert_eq!(priority.cu_price, cu_price);
        }
    }
}
