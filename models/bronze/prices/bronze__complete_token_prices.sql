{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    id,
    symbol,
    decimals,
    price,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id
FROM
    {{ source(
        'silver_crosschain',
        'complete_token_prices'
    ) }}
WHERE
    blockchain = 'optimism'