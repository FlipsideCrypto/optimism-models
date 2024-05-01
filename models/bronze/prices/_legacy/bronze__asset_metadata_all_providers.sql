{{ config (
    materialized = 'view'
) }}

SELECT
    token_address,
    id,
    symbol,
    blockchain,
    provider,
    _unique_key,
    _inserted_timestamp
FROM
    {{ source(
        'silver_crosschain',
        'asset_metadata_all_providers'
    ) }}
WHERE
    blockchain = 'optimism'
