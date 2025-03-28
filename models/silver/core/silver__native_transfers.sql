{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','non_realtime','reorg']
) }}

WITH eth_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        identifier, --deprecate
        from_address,
        to_address,
        value,
        concat_ws(
            '-',
            block_number,
            tx_position,
            CONCAT(
                type,
                '_',
                trace_address
            )
        ) AS _call_id,
        modified_timestamp AS _inserted_timestamp,
        value_precise_raw,
        value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        value > 0
        AND tx_succeeded
        AND trace_succeeded
        AND TYPE NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                eth_base
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    identifier, --deprecate
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address,
    to_address,
    value AS amount,
    value_precise_raw AS amount_precise_raw,
    value_precise AS amount_precise,
    ROUND(
        value * price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'trace_index']
    ) }} AS native_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    eth_base A
    LEFT JOIN {{ ref('silver__complete_token_prices') }}
    p
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND p.token_address = '0x4200000000000000000000000000000000000006'
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
