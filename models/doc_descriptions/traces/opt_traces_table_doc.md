{% docs opt_traces_table_doc %}

This table contains flattened trace data for internal contract calls on the Optimism Blockchain. Hex encoded fields can be decoded to integers by using `ethereum.ethereum.public.udf_hex_to_int()`.

{% enddocs %}

{% docs evm_value_hex %}

The value of the transaction in hexadecimal format.

{% enddocs %}

{% docs evm_trace_error_reason %}

The reason for the trace failure, if any.

{% enddocs %}

{% docs evm_trace_address %}

The trace address for this trace.

{% enddocs %}

{% docs evm_revert_reason %}

The reason for the revert, if available.

{% enddocs %}

{% docs evm_trace_succeeded %}

The boolean value representing if the trace succeeded.

{% enddocs %}