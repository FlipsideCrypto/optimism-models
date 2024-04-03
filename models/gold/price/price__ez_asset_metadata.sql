{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    token_address,
    symbol,
    NAME,
    decimals,
    blockchain,
    blockchain_name,
    blockchain_id,
    is_deprecated,
    complete_token_asset_metadata_id AS ez_asset_metadata_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__complete_token_asset_metadata') }}
