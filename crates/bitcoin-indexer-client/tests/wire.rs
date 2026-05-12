use bitcoin_indexer_client::{
    BitcoinIndexerClient, BitcoinOutputFilter, FilterStream, TxOutput, TxOutputPage,
};

#[test]
fn client_exposes_filter_stream_contract() {
    fn assert_stream<T: FilterStream<Filter = BitcoinOutputFilter, Event = TxOutput>>() {}
    assert_stream::<BitcoinIndexerClient>();
}

#[test]
fn page_type_is_plain_typed_data() {
    let page = TxOutputPage {
        outputs: Vec::new(),
        next_cursor: None,
        has_more: false,
    };

    assert!(!page.has_more);
    assert!(page.outputs.is_empty());
}
