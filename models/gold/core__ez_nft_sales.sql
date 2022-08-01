{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_type,
    platform_address,
    platform_name,
    seller_address,
    buyer_address,
    nft_address,
    tokenId,
    currency_symbol,
    currency_address,
    price,
    price_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature
FROM
    {{ ref('silver__quixotic_sales') }}
