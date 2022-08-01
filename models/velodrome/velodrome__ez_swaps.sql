{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['velodrome']
) }}

SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    platform,
    contract_address,
    pool_address,
    pool_name,
    sender_address,
    to_address,
    amount_in,
    amount_in_usd,
    amount_out,
    amount_out_usd,
    token_address_in,
    token_address_out,
    symbol_in,
    symbol_out,
    token0_symbol,
    token1_symbol,
    lp_fee,
    lp_fee_usd,
    lp_fee_symbol,
    lp_fee_token_address
FROM
    {{ ref('silver__velodrome_swaps') }}
