{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position, --new column
    trace_index,
    from_address,
    to_address,
    input,
    output,
    TYPE,
    trace_address, --new column
    sub_traces,
    DATA,
    VALUE,
    value_precise_raw,
    value_precise,
    value_hex, --new column
    gas,
    gas_used,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    trace_succeeded, --new column
    tx_succeeded, --new column
    error_reason,
    revert_reason, --new column
    fact_traces_id,
    inserted_timestamp,
    modified_timestamp,
    trace_status, --deprecate
    tx_status, --deprecate
    identifier --deprecate
FROM
    {{ ref('silver__fact_traces2') }} 
    --ideal state = source from silver.traces2 and materialize this model as a table (core.fact_traces2)