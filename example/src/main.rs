use graceful_endpoints::{
    EndpointConfig, SendTxConfig,
    jito_rpc::JitoRegion,
    send_txn::service::{TransactionServiceConfig, start_tx_sending_service},
};
use log::info;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    message::{VersionedMessage, legacy::Message as LegacyMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::VersionedTransaction,
};
use std::{collections::HashSet, str::FromStr, sync::Arc, time::Instant};

/// Number of transfer instructions to generate
const TX_COUNT: u64 = 10;
/// Transfer amount
const TRANSFER_AMOUNT: u64 = 1000;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv()?;
    env_logger::init();

    let rpc_url = std::env::var("RPC_URL")?;
    let signer = Arc::new(Keypair::from_base58_string(&std::env::var("KEYPAIR")?));

    let destination = Pubkey::from_str(&std::env::var("DESTINATION_WALLET")?)?;
    let jito_auth = std::env::var("JITO_AUTH_KEYPAIR").ok();
    let service_config = TransactionServiceConfig::builder()
        .jito_use_grpc(jito_auth.is_some())
        .jito_auth(jito_auth)
        .jito_region(JitoRegion::London)
        .build();

    let grpc_url = std::env::var("GRPC_URL").ok();
    let grpc_x_token = std::env::var("GRPC_X_TOKEN").ok();
    let grpc_enabled = grpc_url.is_some();
    let (endpoints, receivers) = EndpointConfig::builder()
        .rpc_url(Some(rpc_url))
        .grpc_url(grpc_url)
        .grpc_x_token(grpc_x_token)
        .subscribe_tx_statuses(
            grpc_enabled.then_some(graceful_endpoints::TxSubscribe::Some(vec![
                signer.pubkey().to_string(),
            ])),
        )
        .build()
        .build_recommended(3, false)
        .await?;

    let (service, _tasks) =
        start_tx_sending_service(&endpoints, &receivers, service_config).await?;

    let (handle, mut confirmation_receiver) = service.subscribe();
    let mut send_ids = HashSet::new();

    let mut send_config = SendTxConfig::default();
    send_config.use_jito = true;
    send_config.cu_price_increment = 500;
    send_config.retries = 2;
    send_config.rpc_config = Some(RpcSendTransactionConfig {
        skip_preflight: true,
        preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
        max_retries: Some(0),
        ..Default::default()
    });

    // Generate SOL transfers
    let instructions = solana_sdk::system_instruction::transfer_many(
        &signer.pubkey(),
        &(0..TX_COUNT)
            .map(|i| (destination, TRANSFER_AMOUNT + i))
            .collect::<Vec<_>>(),
    );
    let mut transactions = Vec::with_capacity(instructions.len());
    for ix in instructions {
        let tx = VersionedTransaction::try_new(
            VersionedMessage::Legacy(LegacyMessage::new(&[ix], Some(&signer.pubkey()))),
            &[&signer],
        )?;
        transactions.push(tx.clone());
    }

    for transaction in transactions {
        let id = handle
            .send_transaction(transaction, &[signer.clone()], send_config)
            .await?;
        send_ids.insert(id);
    }

    let sent = send_ids.len();
    let mut confirmed = 0;
    let mut unconfirmed = 0;
    let mut errors = 0;

    let start = Instant::now();
    while let Some(res) = confirmation_receiver.recv().await {
        if !send_ids.remove(&res.id) {
            info!("duplicate id removal: {}", res.id);
            continue;
        };

        match res.processed {
            Some(_) => {
                confirmed += 1;
                if res.error.is_some() {
                    errors += 0;
                }
            }
            None => {
                unconfirmed += 1;
            }
        }

        if send_ids.is_empty() {
            info!("received all confirmations!");
            break;
        }

        if start.elapsed().as_secs() > 60 {
            info!(
                "60 seconds timeout elapsed. no confirmation for {} txns",
                send_ids.len()
            );
            break;
        }
    }

    info!(
        "confirmed {}/{}, unconfirmed: {}, failed: {}",
        confirmed, sent, unconfirmed, errors
    );
    info!("exiting....");

    Ok(())
}
