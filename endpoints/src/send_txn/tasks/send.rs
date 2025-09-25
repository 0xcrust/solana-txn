use crate::jito_rpc::{JitoClient, JitoRegion};
use crate::send_txn::service::{JitoConnection, TransactionService};
use crate::{SendTransactionData, SingleOrBundled, metrics};
use std::time::Instant;

use ::metrics::{counter, gauge, histogram};
use anyhow::{anyhow, bail};
use either::Either;
use futures::StreamExt;
use futures::future::OptionFuture;
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_searcher_client::send_bundle_no_wait;
use jito_searcher_client::token_authenticator::ClientInterceptor;
use log::{debug, error, trace};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::transaction::VersionedTransaction;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{service::interceptor::InterceptedService, transport::Channel};

const SEND_ATTEMPTS: u8 = 3;

pub fn send_task(
    service: &TransactionService,
    new_txs_receiver: mpsc::Receiver<SendTransactionData>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    let service = service.clone();
    tokio::spawn(async move {
        let stream = ReceiverStream::new(new_txs_receiver).for_each_concurrent(
            service.send_concurrency,
            |data| async {
                match service.process_send(data).await {
                    Err(e) => error!("send error: {}", e),
                    Ok(None) => trace!("transaction was not processed"),
                    Ok(Some(_)) => {}
                }
            },
        );
        stream.await;
        Ok(())
    })
}

impl TransactionService {
    pub async fn process_send(&self, data: SendTransactionData) -> anyhow::Result<Option<()>> {
        let blockhash = self.endpoints.recent_blockhash.read().await.clone();
        let slot = self.endpoints.current_slot.read().await.slot;

        let Some(data) = data.update(blockhash, slot)? else {
            return Ok(None);
        };

        let kind = if data.is_bundled() {
            "bundled"
        } else {
            "single"
        };
        let source = data.source_id.inner().to_string();

        counter!(metrics::TOTAL_SEND_ATTEMPTS, "tx_kind" => kind, "source" => source.clone())
            .increment(1);

        if self.skip_duplicate {
            match &data.data {
                SingleOrBundled::Bundled(data) => {
                    if self.unconfirmed_bundles.contains_key(&data.uuid) {
                        counter!(metrics::UNIQUE_DUPLICATE_TXNS, "source" => source.clone(), "tx_kind" => kind).increment(1);
                        return Ok(None);
                    }
                }
                SingleOrBundled::Single(data) => {
                    if self.unconfirmed_txs.contains_key(&data.signature) {
                        counter!(metrics::UNIQUE_DUPLICATE_TXNS, "source" => source.clone(), "tx_kind" => kind).increment(1);
                        return Ok(None);
                    }
                }
            }
        }

        let jito_transport = match self.jito {
            JitoConnection::Grpc {
                rpc_client: _,
                client: _,
            } => "grpc",
            JitoConnection::Rpc(_) => "rpc",
        };

        let send_rpc_fut = OptionFuture::<_>::from(data.config.use_rpc.then_some(async {
            let res = self.send_to_rpc(&data).await;
            (res, data.sent_at.elapsed())
        }));
        let send_jito_fut = OptionFuture::<_>::from(data.config.use_jito.then_some(async {
            let res = self.send_to_jito(&data).await;
            (res, data.sent_at.elapsed())
        }));

        let (send_rpc_res, send_jito_res) =
            futures::future::join(send_rpc_fut, send_jito_fut).await;

        let sent_rpc = match send_rpc_res {
            None => false,
            Some((Err(e), _)) => {
                error!("Failed to send to rpc: {}", e);
                false
            }
            Some((Ok(_), duration)) => {
                histogram!(metrics::SEND_DELAY_RPC, "source" => source.clone(), "tx_kind" => kind)
                    .record(duration);
                true
            }
        };

        let sent_jito = match send_jito_res {
            None => false,
            Some((Err(e), _)) => {
                error!("Failed to send to jito: {}", e);
                false
            }
            Some((Ok(_), duration)) => {
                histogram!(metrics::SEND_DELAY_JITO, "source" => source.clone(), "tx_kind" => kind, "jito_transport" => jito_transport).record(duration);
                true
            }
        };

        if !(sent_jito || sent_rpc) {
            bail!("no transaction/bundle was sent");
        }

        let print_bool = |b: bool| -> &'static str { if b { "true" } else { "false" } };
        let is_bundled = match &data.data {
            SingleOrBundled::Bundled(_) => {
                debug_assert!(sent_jito);
                true
            }
            SingleOrBundled::Single(_) => sent_jito && self.jito.grpc_enabled(),
        };

        counter!(metrics::TOTAL_SENT, "source" => source.clone(), "tx_kind" => kind, "sent_jito" => print_bool(sent_jito), "sent_rpc" => print_bool(sent_rpc), "jito_transport" => jito_transport, "is_bundled" => print_bool(is_bundled)).increment(1);
        if data.is_first_send() {
            counter!(metrics::UNIQUE_SENT_TXNS, "source" => source.clone(), "tx_kind" => kind)
                .increment(1);
        }

        match &data.data {
            SingleOrBundled::Bundled(bundle) => {
                let uuid = bundle.uuid.clone();
                // use bundle confirmations
                debug_assert!(sent_jito && !sent_rpc);
                self.unconfirmed_bundles
                    .insert(uuid, data.as_pending(false));
            }
            SingleOrBundled::Single(tx) => {
                // use txn confirmations
                self.unconfirmed_txs
                    .insert(tx.signature, data.as_pending(false));

                if sent_jito && self.jito.grpc_enabled() {
                    // if sent as bundles, still track bundle confirmations but only for stats keeping
                    match data.derive_bundle_id() {
                        Ok(id) => {
                            self.unconfirmed_bundles.insert(id, data.as_pending(true));
                        }
                        Err(e) => {
                            error!("bug: failed to derive bundle id: {}", e);
                        }
                    }
                }
            }
        }

        self.tx_queue.insert(data.id, data);
        gauge!(metrics::TX_QUEUE_COUNT).set(self.tx_queue.len() as f64);
        let size = self
            .tx_queue
            .iter()
            .map(|x| std::mem::size_of_val(x.value()))
            .sum::<usize>();
        gauge!(metrics::TX_QUEUE_SIZE).set(size as f64);

        Ok(Some(()))
    }

    /// Send transaction to Jito
    pub async fn send_to_jito(&self, data: &SendTransactionData) -> anyhow::Result<()> {
        self.jito.send(data).await
    }

    /// Send transaction to RPC
    pub async fn send_to_rpc(&self, data: &SendTransactionData) -> anyhow::Result<()> {
        match &data.data {
            SingleOrBundled::Single(tx_data) => {
                send_rpc_transaction(
                    &self.send_rpc,
                    &tx_data.tx,
                    data.config.rpc_config.unwrap_or(self.rpc_config),
                )
                .await?
            }
            SingleOrBundled::Bundled(_) => return Err(anyhow!("cannot send bundled txn via RPC")),
        }
        Ok(())
    }
}

impl JitoConnection {
    /// Send a transaction to the network. Returns `true` if sent successfully
    pub async fn send(&self, data: &SendTransactionData) -> anyhow::Result<()> {
        match self {
            JitoConnection::Rpc(client) => match &data.data {
                SingleOrBundled::Single(data) => {
                    send_jito_rpc_transaction(client, &data.tx, None).await?
                }
                SingleOrBundled::Bundled(data) => {
                    let txs = data.bundle.iter().map(|b| &b.tx).collect::<Vec<_>>();
                    send_jito_rpc_bundle(client, &txs, None).await?;
                }
            },
            JitoConnection::Grpc {
                rpc_client: _,
                client,
            } => {
                let txs = match &data.data {
                    SingleOrBundled::Bundled(data) => {
                        &data.bundle.iter().map(|b| b.tx.clone()).collect::<Vec<_>>()[..]
                    }
                    SingleOrBundled::Single(data) => &[data.tx.clone()],
                };

                let uuid = match client.write().await.as_mut() {
                    Either::Left(client) => send_jito_grpc_no_auth(client, txs).await?,
                    Either::Right(client) => send_jito_grpc(client, txs).await?,
                };

                if let SingleOrBundled::Bundled(data) = &data.data {
                    debug_assert!(data.uuid == uuid);
                }
            }
        }

        Ok(())
    }
}

async fn send_jito_grpc(
    client: &mut SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    tx: &[VersionedTransaction],
) -> anyhow::Result<String> {
    let start = Instant::now();

    let mut error = anyhow!("failed jito auth grpc send: unspecified");
    for i in 1..=SEND_ATTEMPTS {
        match send_bundle_no_wait(tx, client).await {
            Ok(response) => {
                debug!("Sent RPC bundle to jito after {} ms", duration_f64(start));
                return Ok(response.get_ref().uuid.clone());
            }
            Err(status) => {
                error!(
                    "{i}/{SEND_ATTEMPTS} failed jito-rpc send: {}",
                    status.message()
                );
                error = anyhow!(status);
            }
        }
    }

    Err(error)
}

async fn send_jito_grpc_no_auth(
    client: &mut SearcherServiceClient<Channel>,
    tx: &[VersionedTransaction],
) -> anyhow::Result<String> {
    let start = Instant::now();

    let mut error = anyhow!("failed jito grpc send: unspecified");
    for i in 1..=SEND_ATTEMPTS {
        match send_bundle_no_wait(tx, client).await {
            Ok(response) => {
                debug!("Sent RPC bundle to jito after {} ms", duration_f64(start));
                return Ok(response.get_ref().uuid.clone());
            }
            Err(status) => {
                error!(
                    "{i}/{SEND_ATTEMPTS} failed jito-rpc send: {}",
                    status.message()
                );
                error = anyhow!(status);
            }
        }
    }

    Err(error)
}

async fn send_jito_rpc_transaction(
    jito: &JitoClient,
    tx: &VersionedTransaction,
    region_override: Option<JitoRegion>,
) -> anyhow::Result<()> {
    let start = Instant::now();

    for i in 1..=SEND_ATTEMPTS {
        match jito.send_transaction(tx, region_override).await {
            Ok(_) => {
                debug!("Sent RPC bundle to jito after {} ms", duration_f64(start));
                return Ok(());
            }
            Err(e) => {
                error!("{i}/{SEND_ATTEMPTS} failed jito-rpc send: {e}");
                if i == SEND_ATTEMPTS {
                    return Err(anyhow!(e));
                }
            }
        }
    }

    Ok(())
}

async fn send_jito_rpc_bundle(
    jito: &JitoClient,
    txs: &[&VersionedTransaction],
    region_override: Option<JitoRegion>,
) -> anyhow::Result<()> {
    let start = Instant::now();

    for i in 1..=SEND_ATTEMPTS {
        match jito.send_bundle(txs, region_override).await {
            Ok(_) => {
                debug!("Sent RPC bundle to jito after {} ms", duration_f64(start));
                return Ok(());
            }
            Err(e) => {
                error!("{i}/{SEND_ATTEMPTS} failed jito-rpc send: {e}");
                if i == SEND_ATTEMPTS {
                    return Err(anyhow!(e));
                }
            }
        }
    }

    Ok(())
}

async fn send_rpc_transaction(
    rpc_client: &RpcClient,
    tx: &VersionedTransaction,
    config: RpcSendTransactionConfig,
) -> anyhow::Result<()> {
    let start = Instant::now();

    for i in 1..=SEND_ATTEMPTS {
        match rpc_client.send_transaction_with_config(tx, config).await {
            Ok(_) => {
                debug!("Sent tx to RPC after {} ms", duration_f64(start));
                return Ok(());
            }
            Err(e) => {
                error!("{i}/{SEND_ATTEMPTS} failed rpc send: {e}");
                if i == SEND_ATTEMPTS {
                    return Err(anyhow!(e));
                }
            }
        }
    }

    Ok(())
}

fn duration_f64(instant: Instant) -> f64 {
    instant.elapsed().as_micros() as f64 / 1000.0
}
