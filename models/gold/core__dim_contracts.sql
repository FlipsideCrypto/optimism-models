{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    contract_address,
    token_symbol AS symbol,
    token_name AS NAME,
    token_decimals AS decimals,
    contract_address AS address
FROM
    {{ ref('silver__contracts') }}
