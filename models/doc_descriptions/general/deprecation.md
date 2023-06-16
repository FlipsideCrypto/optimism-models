{% docs deprecation %}

Deprecating soon: This is a notice that we're only removing the below columns. Please migrate queries using these columns to `fact_decoded_event_logs`, `ez_decoded_event_logs` or use manual parsing of topics and data. The following columns will be deprecated on 7/16/23:

`Fact_event_logs` Columns:
- `event_name`
- `event_inputs`
- `contract_name`

`Fact_transactions` Columns:
- `tx_json`
{% enddocs %}