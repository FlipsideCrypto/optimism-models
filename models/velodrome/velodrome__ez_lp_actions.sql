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
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    pool_address,
    pool_name,
    pool_type,
    sender_address,
    lp_action,
    token0_symbol,
    token1_symbol,
    token0_amount,
    token1_amount,
    token0_amount_usd,
    token1_amount_usd,
    token0_address,
    token1_address,
    lp_token_action,
    lp_token_amount,
    lp_token_amount_usd
FROM
    {{ ref('silver__velodrome_lp_actions') }}
