use bitcoin::Txid;
use bitcoin_receipt_watcher_client::{
    BitcoinReceipt, BitcoinReceiptWatcherClient, ByIdLookup, WatchEvent, WatchStatus,
};

#[test]
fn client_exposes_by_id_lookup_contract() {
    fn assert_lookup<T: ByIdLookup<Id = Txid, Receipt = BitcoinReceipt>>() {}
    assert_lookup::<BitcoinReceiptWatcherClient>();
}

#[test]
fn pending_event_wire_shape_decodes() {
    let event: WatchEvent = serde_json::from_str(
        r#"{
            "chain":"bitcoin",
            "txid":"3333333333333333333333333333333333333333333333333333333333333333",
            "requesting_operation_id":"operation-1",
            "status":"pending",
            "receipt":null
        }"#,
    )
    .unwrap();

    assert_eq!(event.status, WatchStatus::Pending);
    assert!(event.receipt.is_none());
}
