use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

// gauge
pub const TX_QUEUE_COUNT: &str = "tx_data_queue_count";
pub const TX_QUEUE_SIZE: &str = "tx_data_queue_size_bytes";

// counter
pub const UNIQUE_ATTEMPTED_TXNS: &str = "unique_attempted_txns";
pub const UNIQUE_DUPLICATE_TXNS: &str = "unique_duplicate_txns_skipped";
pub const UNIQUE_SENT_TXNS: &str = "unique_sent_txns";
pub const UNIQUE_LANDED_TXNS: &str = "unique_landed_txns";
pub const UNIQUE_SUCCESS_TXNS: &str = "unique_success_txns";

pub const TOTAL_SEND_ATTEMPTS: &str = "send_txn_attempts_total";
pub const TOTAL_SENT: &str = "sent_txns_total";

pub const TOTAL_LANDED_BUNDLES: &str = "landed_bundles_total";

pub const TOTAL_ACCEPTED_BUNDLES: &str = "accepted_bundles_total";
pub const DROPPED_BUNDLES: &str = "dropped_bundles";
pub const INTERNAL_ERROR_BUNDLES: &str = "internal_error_bundles";
pub const SIMULATION_FAILURE_BUNDLES: &str = "simulation_failure_bundles";
pub const STATE_AUCTION_BID_REJECTED_BUNDLES: &str = "state_auction_bid_rejected_bundles";
pub const WINNING_BATCH_BID_REJECTED_BUNDLES: &str = "winning_batch_bid_rejected_bundles";

// histograms
pub const SEND_DELAY_JITO: &str = "send_delay";
pub const SEND_DELAY_RPC: &str = "send_delay";

pub const CONFIRMATION_DELAY: &str = "confirmation_delay";
pub const LANDED_ATTEMPTS: &str = "landed_attempts";
pub const LANDED_TIPS: &str = "landed_tips";
pub const LANDED_PRIOFEE: &str = "landed_priofee";

pub fn setup() {
    describe_gauge!(TX_QUEUE_COUNT, "Total number of queued transactions");
    describe_gauge!(
        TX_QUEUE_SIZE,
        Unit::Bytes,
        "Total size of queued transactions"
    );

    describe_counter!(
        UNIQUE_ATTEMPTED_TXNS,
        "Number of unique attempted transactions"
    );
    describe_counter!(
        UNIQUE_DUPLICATE_TXNS,
        "Number of transactions not processed due to being duplicates"
    );
    describe_counter!(
        UNIQUE_SENT_TXNS,
        "Number of transactions sent at least once"
    );
    describe_counter!(UNIQUE_LANDED_TXNS, "Number of transactions that landed");
    describe_counter!(
        UNIQUE_SUCCESS_TXNS,
        "Number of transactions that were successful"
    );

    describe_counter!(
        TOTAL_SEND_ATTEMPTS,
        "Total number of times we attempted a send"
    );
    describe_counter!(TOTAL_SENT, "Total number of successfully sent transactions");

    describe_counter!(TOTAL_LANDED_BUNDLES, "Total number of landed bundles");
    describe_counter!(
        TOTAL_ACCEPTED_BUNDLES,
        "Bundles accepted by the block-engine and forwarded to a jito-solana validator"
    );
    describe_counter!(
        DROPPED_BUNDLES,
        "Bundles dropped because there is no upcoming leader"
    );
    describe_counter!(
        INTERNAL_ERROR_BUNDLES,
        "Bundles dropped due to an internal error"
    );
    describe_counter!(
        SIMULATION_FAILURE_BUNDLES,
        "Bundle dropped due to a simulation error"
    );
    describe_counter!(
        STATE_AUCTION_BID_REJECTED_BUNDLES,
        "Bundle's bid was not high enough to be included in its state auction's set of winners."
    );
    describe_counter!(
        WINNING_BATCH_BID_REJECTED_BUNDLES,
        "Bundle's bid was high enough to win its state auction. However, not high enough relative to other state auction winners"
    );

    describe_histogram!(
        CONFIRMATION_DELAY,
        "Confirmation delay for a transaction, measured in slots"
    );
    describe_histogram!(
        SEND_DELAY_RPC,
        Unit::Seconds,
        "How long it took to send a rpc transaction"
    );
    describe_histogram!(
        SEND_DELAY_JITO,
        Unit::Seconds,
        "How long it took to send a jito transaction"
    );
    describe_histogram!(
        LANDED_ATTEMPTS,
        "Number of attempts before a transaction landed"
    );
    describe_histogram!(LANDED_TIPS, "Tip amount paid for a landed transaction");
    describe_histogram!(LANDED_PRIOFEE, "Priority fee paid for a landed transaction");
}
