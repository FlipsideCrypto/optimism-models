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
    staking_action_type,
    lp_token_amount,
    lp_provider_address,
    gauge_address,
    pool_address,
    pool_name,
    pool_type,
    token0_symbol,
    token1_symbol,
    token0_address,
    token1_address
FROM
    {{ ref('silver__velodrome_staking_actions') }}
