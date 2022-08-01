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
    unlock_date,
    token_id,
    velo_amount,
    velo_amount_usd,
    deposit_type
FROM
    {{ ref('silver__velodrome_locks') }}
