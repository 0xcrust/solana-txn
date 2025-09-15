use std::sync::atomic::{AtomicUsize, Ordering};

/// Unique id incremented for each transaction that is generated and sent through the wire.
static NEXT_TRANSACTION_ID: AtomicUsize = AtomicUsize::new(1);

/// Incremented for each service that sends transactions through the wire.
///
/// It is used by the transaction-sending service to send confirmation for a txn
/// back to its specific originator.
static NEXT_TRANSACTION_SOURCE_ID: AtomicUsize = AtomicUsize::new(1);

/// The Unique ID of a single sent-transaction.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct TransactionId(usize);

impl TransactionId {
    /// Creates a new `TransactionId`.
    pub fn next() -> Self {
        TransactionId(NEXT_TRANSACTION_ID.fetch_add(1, Ordering::SeqCst))
    }
}

impl std::fmt::Display for TransactionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TransactionId({:?})", self.0)
    }
}

/// The ID of a single source that generates transaction
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TransactionSourceId(usize);

impl TransactionSourceId {
    /// Creates a new `TransactionSourceId`.
    pub fn next() -> Self {
        TransactionSourceId(NEXT_TRANSACTION_SOURCE_ID.fetch_add(1, Ordering::SeqCst))
    }

    pub fn inner(&self) -> usize {
        self.0
    }
}

impl std::fmt::Display for TransactionSourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "source({})", self.0)
    }
}
