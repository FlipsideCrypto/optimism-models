{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['velodrome']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    provider_address,
    velo_action,
    unlock_date,
    token_id,
    velo_amount,
    ROUND(
        velo_amount * price,
        2
    ) AS velo_amount_usd,
    deposit_type
FROM
    {{ ref('silver__velodrome_locks') }}
    LEFT JOIN {{ ref('silver__prices') }}
    prices
    ON HOUR = DATE_TRUNC(
        'hour',
        block_timestamp
    )
WHERE
    symbol = 'VELO'
