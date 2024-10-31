{% docs opt_logs_table_doc %}

This table contains flattened event logs from transactions on the Optimism Blockchain. Transactions may have multiple events, which are denoted by the event index for a transaction hash. Therefore, this table is unique on the combination of transaction hash and event index. Please see `fact_decoded_event_logs` or `ez_decoded_event_logs` for the decoded event logs.

{% enddocs %}

{% docs evm_topic_0 %}

The first topic of the event, which is a unique identifier for the event.

{% enddocs %}


{% docs evm_topic_1 %}  

The second topic of the event, if applicable.

{% enddocs %}


{% docs evm_topic_2 %}

The third topic of the event, if applicable.

{% enddocs %}


{% docs evm_topic_3 %}

The fourth topic of the event, if applicable.  

{% enddocs %}