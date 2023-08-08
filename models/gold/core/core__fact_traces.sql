{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value,
    IFNULL(
        eth_value_precise_raw,
        '0'
    ) AS eth_value_precise_raw,
    IFNULL(
        eth_value_precise,
        '0'
    ) AS eth_value_precise,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    trace_status,
    error_reason,
    trace_index
FROM
    (
        SELECT
            tx_hash,
            block_number,
            block_timestamp,
            from_address,
            to_address,
            eth_value,
            gas,
            gas_used,
            input,
            output,
            TYPE,
            identifier,
            DATA,
            tx_status,
            sub_traces,
            trace_status,
            error_reason,
            trace_index,
            REPLACE(
                DATA :value :: STRING,
                '0x'
            ) AS hex,
            to_varchar(TO_NUMBER(hex, REPEAT('X', LENGTH(hex)))) AS eth_value_precise_raw,
            IFF(LENGTH(eth_value_precise_raw) > 18, LEFT(eth_value_precise_raw, LENGTH(eth_value_precise_raw) - 18) || '.' || RIGHT(eth_value_precise_raw, 18), '0.' || LPAD(eth_value_precise_raw, 18, '0')) AS rough_conversion,
            IFF(
                POSITION(
                    '.000000000000000000' IN rough_conversion
                ) > 0,
                LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
                REGEXP_REPLACE(
                    rough_conversion,
                    '0*$',
                    ''
                )
            ) AS eth_value_precise
        FROM
            {{ ref('silver__traces') }}
    )
