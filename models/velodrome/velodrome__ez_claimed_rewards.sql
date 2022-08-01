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
    reward_type,
    token_id,
    claimed_amount,
    claimed_amount_usd,
    token_symbol,
    token_address,
    claim_epoch,
    max_epoch
FROM
    {{ ref('silver__velodrome_claimed_rewards') }}
