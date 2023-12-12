{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    contract_address,
    DATA AS abi,
    abi_source,
    bytecode,
    abis_id AS dim_contract_abis_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__abis') }}
