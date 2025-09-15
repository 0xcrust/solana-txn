use crate::grpc::bundle_results::start_grpc_bundle_results_task;
use crate::grpc::confirmation::start_grpc_confirmation_task;

use crate::jito_rpc::{JitoClient, JitoRegion};
use crate::rpc::bundle_confirmation::start_rpc_bundle_confirmation_task;
use crate::rpc::confirmation::start_rpc_confirmation_task;
use crate::send_txn::tasks::process::process_task;
use crate::send_txn::tasks::send::send_task;
use crate::send_txn::types::{SendTransactionData, SendTxConfig};
use crate::{
    ClusterEndpoints, ClusterReceivers, ConfirmationResult, ExecutionResult, PendingTransaction,
    TransactionId, TransactionSourceId, metrics,
};
use std::{sync::Arc, time::Duration};

use ::metrics::counter;
use anyhow::bail;
use builder_rs::Builder;
use dashmap::DashMap;
use either::Either;
use jito_protos::searcher::searcher_service_client::SearcherServiceClient;
use jito_searcher_client::{
    get_searcher_client_auth, get_searcher_client_no_auth, token_authenticator::ClientInterceptor,
};
use serde::Deserialize;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcSendTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{Keypair, Signature},
    transaction::VersionedTransaction,
};
use tokio::sync::RwLock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::{service::interceptor::InterceptedService, transport::Channel};

/// Maximum number of transactions in a bundle
const MAX_BUNDLE_LENGTH: usize = 5;

/// The transaction service handle
#[derive(Clone)]
pub struct TransactionService {
    /// Solana network information
    pub(crate) endpoints: ClusterEndpoints,

    /// Rpc client for sending transactions
    pub(crate) send_rpc: Arc<RpcClient>,

    /// Jito client
    pub(crate) jito: JitoConnection,

    /// Holds information for each handle connected to this service
    pub(crate) handles: Arc<DashMap<TransactionSourceId, HandleConfig>>,

    /// Maintains a queue of unresolved transaction data
    pub(crate) tx_queue: Arc<DashMap<TransactionId, SendTransactionData>>,

    /// Maintains a queue of unconfirmed bundles
    pub(crate) unconfirmed_bundles: Arc<DashMap<String, PendingTransaction>>,

    /// Maintains a queue of unconfirmed transaction
    pub(crate) unconfirmed_txs: Arc<DashMap<Signature, PendingTransaction>>,

    /// Rpc config
    pub(crate) rpc_config: RpcSendTransactionConfig,

    /// Handle for scheduling transactions
    pub(crate) tx_sender: mpsc::Sender<SendTransactionData>,

    /// Whether to skip duplicate txns or bundles
    pub(crate) skip_duplicate: bool,
}

/// Unauthenticated / Authenticated
pub type JitoGrpcConnection = Either<
    SearcherServiceClient<Channel>,
    SearcherServiceClient<InterceptedService<Channel, ClientInterceptor>>,
>;

/// Different flavours of Jito connections
#[derive(Clone)]
pub enum JitoConnection {
    /// RPC
    Rpc(Arc<JitoClient>),
    /// GRPC
    Grpc {
        rpc_client: Arc<JitoClient>,
        client: Arc<RwLock<JitoGrpcConnection>>,
    },
}

impl JitoConnection {
    pub fn rpc(&self) -> &Arc<JitoClient> {
        match self {
            JitoConnection::Rpc(client) => client,
            JitoConnection::Grpc {
                rpc_client,
                client: _,
            } => rpc_client,
        }
    }

    pub fn grpc_enabled(&self) -> bool {
        matches!(
            self,
            JitoConnection::Grpc {
                rpc_client: _,
                client: _
            }
        )
    }
}

/// Configuration values for send service
#[derive(Builder, Debug, Clone, Deserialize)]
pub struct TransactionServiceConfig {
    pub send_rpc_url: Option<String>,
    pub jito_use_grpc: bool,
    pub jito_region: JitoRegion,
    pub jito_auth: Option<String>,
    pub check_confirmation_frequency: Option<Duration>,
    pub rpc_config: Option<RpcSendTransactionConfig>,
    pub skip_duplicate: Option<bool>,
}

/// Handle for communicating with the transaction service
#[derive(Clone)]
pub struct SendHandle {
    id: TransactionSourceId,
    tx_sender: mpsc::Sender<SendTransactionData>,
}

/// Internal state for each source handle
pub struct HandleConfig {
    pub result_sender: mpsc::Sender<ExecutionResult>,
}

pub struct Channels {
    pub new_txs_receiver: mpsc::Receiver<SendTransactionData>,
    pub tx_confirmation_sender: mpsc::Sender<ConfirmationResult>,
    pub tx_confirmation_receiver: mpsc::Receiver<ConfirmationResult>,
    pub bundle_confirmation_sender: mpsc::Sender<ConfirmationResult>,
    pub bundle_confirmation_receiver: mpsc::Receiver<ConfirmationResult>,
}

impl TransactionService {
    pub async fn init(
        endpoints: &ClusterEndpoints,
        config: TransactionServiceConfig,
    ) -> anyhow::Result<(Self, Channels)> {
        let send_rpc = config
            .send_rpc_url
            .as_ref()
            .map(|url| Arc::new(RpcClient::new(url.clone())))
            .unwrap_or(endpoints.rpc.clone());
        let jito_auth = match config.jito_auth.as_ref() {
            Some(s) => Some(Arc::new(Keypair::try_from(
                solana_sdk::bs58::decode(s).into_vec()?.as_slice(),
            )?)),
            None => None,
        };

        let jito_rpc = Arc::new(JitoClient::new(config.jito_region));

        let jito = match (config.jito_use_grpc, jito_auth) {
            (false, _) => JitoConnection::Rpc(jito_rpc),
            (true, None) => {
                let client = get_searcher_client_no_auth(config.jito_region.urls().url).await?;
                JitoConnection::Grpc {
                    rpc_client: jito_rpc,
                    client: Arc::new(RwLock::new(Either::Left(client))),
                }
            }
            (true, Some(auth)) => {
                let client = get_searcher_client_auth(config.jito_region.urls().url, &auth).await?;
                JitoConnection::Grpc {
                    rpc_client: jito_rpc,
                    client: Arc::new(RwLock::new(Either::Right(client))),
                }
            }
        };

        let (tx_sender, new_txs_receiver) = mpsc::channel(200);
        let (tx_confirmation_sender, tx_confirmation_receiver) = mpsc::channel(200);
        let (bundle_confirmation_sender, bundle_confirmation_receiver) = mpsc::channel(200);

        let service = TransactionService {
            endpoints: endpoints.clone(),
            send_rpc,
            jito,
            handles: Arc::new(DashMap::new()),
            tx_queue: Arc::new(DashMap::new()),
            unconfirmed_bundles: Arc::new(DashMap::new()),
            unconfirmed_txs: Arc::new(DashMap::new()),
            rpc_config: config.rpc_config.unwrap_or(RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
                max_retries: Some(0),
                ..Default::default()
            }),
            skip_duplicate: config.skip_duplicate.unwrap_or(true),
            tx_sender,
        };

        let channels = Channels {
            new_txs_receiver,
            tx_confirmation_sender,
            tx_confirmation_receiver,
            bundle_confirmation_receiver,
            bundle_confirmation_sender,
        };

        Ok((service, channels))
    }

    /// Create a new handle for sending transactions, assigned its own unique id.
    ///
    /// Also returns a channel for receiving results for transactions generated by this handle.
    pub fn subscribe(&self) -> (SendHandle, mpsc::Receiver<ExecutionResult>) {
        let id = TransactionSourceId::next();
        let handle = SendHandle {
            id,
            tx_sender: self.tx_sender.clone(),
        };
        let (result_sender, result_rx) = mpsc::channel(200);
        self.handles
            .insert(handle.id, HandleConfig { result_sender });
        (handle, result_rx)
    }
}

impl SendHandle {
    /// Queue a transaction to be sent to the network
    pub async fn send_transaction(
        &self,
        tx: VersionedTransaction,
        signers: &[Arc<Keypair>],
        config: SendTxConfig,
    ) -> anyhow::Result<TransactionId> {
        counter!(metrics::UNIQUE_ATTEMPTED_TXNS, "tx_kind" => "single").increment(1);

        if !config.use_rpc && !config.use_jito {
            bail!("no target specified for txn send");
        }

        let data = SendTransactionData::new_single(tx, signers, self.id, config);
        let id = data.id;

        self.tx_sender.send(data).await?;

        Ok(id)
    }

    /// Queue a bundle to be sent to the network
    pub async fn send_bundle(
        &self,
        txs: Vec<(VersionedTransaction, &[Arc<Keypair>])>,
        config: SendTxConfig,
    ) -> anyhow::Result<TransactionId> {
        counter!(metrics::UNIQUE_ATTEMPTED_TXNS, "tx_kind" => "bundled").increment(1);

        if !config.use_rpc && !config.use_jito {
            bail!("no target specified for txn send");
        }

        if txs.is_empty() {
            bail!("no transaction specified for bundle")
        }

        if txs.len() > MAX_BUNDLE_LENGTH {
            bail!("exceeded max bundle length. max={MAX_BUNDLE_LENGTH}")
        }

        let data = SendTransactionData::new_bundled(txs, self.id, config);

        let id = data.id;
        self.tx_sender.send(data).await?;

        Ok(id)
    }
}

// todo: implement leader forwarding
pub async fn start_tx_sending_service(
    endpoints: &ClusterEndpoints,
    receivers: &ClusterReceivers,
    config: TransactionServiceConfig,
) -> anyhow::Result<(TransactionService, Vec<JoinHandle<anyhow::Result<()>>>)> {
    let check_confirmation_frequency = config.check_confirmation_frequency;
    let (service, channels) = TransactionService::init(endpoints, config).await?;
    let send_task = send_task(&service, channels.new_txs_receiver);
    let post_processing_task = process_task(
        &service,
        channels.tx_confirmation_receiver,
        channels.bundle_confirmation_receiver,
    );

    let tx_confirmation_task = if let Some(ref grpc_txns) = receivers.tx_status_streamer {
        start_grpc_confirmation_task(
            endpoints.current_block_height.clone(),
            service.unconfirmed_txs.clone(),
            channels.tx_confirmation_sender,
            grpc_txns.resubscribe(),
            check_confirmation_frequency,
        )
    } else {
        start_rpc_confirmation_task(
            endpoints.rpc.clone(),
            endpoints.current_block_height.clone(),
            service.unconfirmed_txs.clone(),
            channels.tx_confirmation_sender,
            Some(CommitmentConfig::confirmed()),
            check_confirmation_frequency,
        )
    };

    let bundle_confirmation_task = start_rpc_bundle_confirmation_task(
        service.jito.rpc().clone(),
        endpoints.current_block_height.clone(),
        service.unconfirmed_bundles.clone(),
        channels.bundle_confirmation_sender,
        Some(CommitmentConfig::confirmed()),
        check_confirmation_frequency,
    );

    let bundle_results_task = match &service.jito {
        JitoConnection::Grpc {
            rpc_client: _,
            client,
        } => Some(start_grpc_bundle_results_task(
            service.unconfirmed_bundles.clone(),
            client.clone(),
        )),
        JitoConnection::Rpc(_) => None,
    };

    let mut tasks = vec![
        send_task,
        post_processing_task,
        tx_confirmation_task,
        bundle_confirmation_task,
    ];

    if let Some(task) = bundle_results_task {
        tasks.push(task);
    }

    Ok((service, tasks))
}
