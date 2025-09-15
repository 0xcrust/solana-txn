use agave_feature_set::FeatureSet;
use solana_bincode::limited_deserialize;
use solana_compute_budget::compute_budget_limits::DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT;
use solana_compute_budget_instruction::instructions_processor::process_compute_budget_instructions;
use solana_pubkey::Pubkey;
use solana_sdk::instruction::CompiledInstruction;
use solana_sdk::message::VersionedMessage;
use solana_sdk::system_instruction::SystemInstruction;
use solana_sdk::transaction::VersionedTransaction;
use solana_sdk_ids::system_program;
use solana_svm_transaction::instruction::SVMInstruction;

#[derive(Copy, Debug, Clone)]
pub struct PriorityFee {
    /// The priority(excluding base fees) paid for this transaction
    pub fee: u64,
    /// The compute unit limit for this transaction
    pub cu_limit: u32,
    /// The compute unit price for this transaction
    pub cu_price: u64,
    /// The tip amount paid for this transaction
    pub tip: u64,
}

impl PriorityFee {
    const BASE_FEE: u64 = 5000;

    pub fn cost(&self) -> u64 {
        self.fee + self.tip
    }

    pub fn total_cost_with_base(&self, signature_count: usize) -> u64 {
        self.cost() + (Self::BASE_FEE * signature_count as u64)
    }
}

impl Default for PriorityFee {
    fn default() -> Self {
        PriorityFee {
            fee: 0,
            cu_limit: DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT,
            cu_price: 0,
            tip: 0,
        }
    }
}

impl PriorityFee {
    pub fn new(fee: u64, cu_limit: u32, cu_price: u64, tip: u64) -> Self {
        PriorityFee {
            fee,
            cu_limit,
            cu_price,
            tip,
        }
    }

    pub fn from_versioned_transaction(
        transaction: &VersionedTransaction,
        tip_accounts: &[Pubkey],
    ) -> Self {
        if transaction.sanitize().is_err() {
            return PriorityFee::default();
        }
        priority_details_from_instructions(
            transaction.message.instructions(),
            transaction.message.static_account_keys(),
            tip_accounts,
        )
    }

    pub fn from_versioned_message(message: &VersionedMessage, tip_accounts: &[Pubkey]) -> Self {
        if message.sanitize().is_err() {
            return PriorityFee::default();
        }
        priority_details_from_instructions(
            message.instructions(),
            message.static_account_keys(),
            tip_accounts,
        )
    }

    pub fn from_instructions(
        instructions: &[CompiledInstruction],
        static_account_keys: &[Pubkey],
        tip_accounts: &[Pubkey],
    ) -> Self {
        priority_details_from_instructions(instructions, static_account_keys, tip_accounts)
    }
}

pub fn get_tip_amount<'a>(
    account_keys: &[Pubkey],
    instructions: impl Iterator<Item = (&'a Pubkey, SVMInstruction<'a>)>,
    tip_accounts: &[Pubkey],
) -> u64 {
    instructions
        .filter_map(|(program, ix)| {
            if *program != system_program::ID {
                return None;
            }
            match limited_deserialize(ix.data, std::mem::size_of::<SystemInstruction>() as u64) {
                Ok(SystemInstruction::Transfer { lamports }) => {
                    let account = account_keys.get(ix.accounts[1] as usize)?;
                    tip_accounts.contains(account).then_some(lamports)
                }
                _ => None,
            }
        })
        .sum()
}

fn priority_details_from_instructions(
    instructions: &[CompiledInstruction],
    static_account_keys: &[Pubkey],
    tip_accounts: &[Pubkey],
) -> PriorityFee {
    let instructions = instructions.iter().filter_map(|ix| {
        let program_id = static_account_keys.get(ix.program_id_index as usize)?;
        let ix = SVMInstruction::from(ix);
        Some((program_id, ix))
    });
    let feature_set = FeatureSet::default();
    let tip = get_tip_amount(static_account_keys, instructions.clone(), tip_accounts);
    let limits =
        process_compute_budget_instructions(instructions, &feature_set).unwrap_or_default();
    PriorityFee {
        fee: limits.get_prioritization_fee(),
        cu_limit: limits.compute_unit_limit,
        cu_price: limits.compute_unit_price,
        tip,
    }
}

#[cfg(test)]
mod test {
    use super::PriorityFee;
    use std::str::FromStr;

    use solana_client::nonblocking::rpc_client::RpcClient;
    use solana_client::rpc_config::RpcTransactionConfig;
    use solana_sdk::commitment_config::CommitmentConfig;
    use solana_sdk::signature::Signature;
    use solana_transaction_status::UiTransactionEncoding;

    #[derive(Debug)]
    struct TransactionDetails {
        fee: u64, // in lamports
        cu_limit: u32,
        cu_price: u64, // in microlamports
    }
    impl TransactionDetails {
        const fn new(fee: u64, cu_limit: u32, cu_price: u64) -> Self {
            TransactionDetails {
                fee,
                cu_limit,
                cu_price,
            }
        }
    }

    const FIXTURES: [(&str, TransactionDetails); 6] = [
        (
            "2pstHqTYrrJnBfvj5pmd1jZLaNo11Dm1sEdYUVWPEPd7tJx5T98YaeJoFcvK2Q3DXUzti6unMBccE9zw8yB6NQsd",
            TransactionDetails::new(105000, 1400000, 71428),
        ),
        (
            "5MVAmz9EA6xzpsJiMo9dTy1HTx33Rmq3796gdUK2uACokBu8dmQzrVp1HKMZuS8UL4JDiNmGFBXCxHuU46A1wxC1",
            TransactionDetails::new(5039, 3631, 10500),
        ),
        (
            "234tKjmeRnw6cNTskBC4CiLUg8AAEdkDCr88mQatkPwp5LA4NSsuCCFFqh4mTUUUyzKqvqKrHxFm3kr3vsEp5pyE",
            TransactionDetails::new(33000, 1400000, 20000),
        ),
        (
            "3UuztBgJFZjVghYtDUKzUhw7s61umxdzXx4d6iVGMKfMNGjRNRBUe3Kr3qg9G2ND6mSHbrzhe2pvZrmpPBRiszr5",
            TransactionDetails::new(33003, 1400000, 20002),
        ),
        (
            "32GVtufP4Lw3osHwK3wqru3NfUTjdPr2EohHfBrmcMVKfRFHRJrMheGmxRZ6poP6QofF8QXj4sdLT1oQVYFJGMUq",
            TransactionDetails::new(5018, 1651, 10500),
        ),
        (
            "3YMo98JnYHEhTJnrecGujGeYCgvuDt3Qwjy9GBhYZqWxxC4SGneSaRcAtsBq9b4udV9gLQ4h1xbzeBLfhXqwjvAa",
            TransactionDetails::new(5039, 3631, 10500),
        ),
    ];
    const BASE_FEE_LAMPORTS: u64 = 5000;
    #[tokio::test]
    async fn test_compute_priority_details() {
        let client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());

        for (signature, details) in FIXTURES {
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

            let transaction = transaction.transaction.transaction.decode().unwrap();
            let priority_details = PriorityFee::from_versioned_transaction(&transaction, &[]);
            let expected_priority_fee = details.fee - BASE_FEE_LAMPORTS;
            assert_eq!(priority_details.cu_limit, details.cu_limit);
            assert_eq!(priority_details.cu_price, details.cu_price);
            assert_eq!(priority_details.fee, expected_priority_fee);
        }
    }
}
