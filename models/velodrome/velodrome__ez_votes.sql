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
    gauge_address,
    external_bribe_address,
    internal_bribe_address,
    pool_address,
    pool_name,
    from_address,
    token_id,
    vote_amount,
    vote_action
FROM
    {{ ref('silver__velodrome_votes') }}
