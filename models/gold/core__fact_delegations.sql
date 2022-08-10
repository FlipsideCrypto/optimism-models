{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    status, 
    delegator, 
    to_delegate, 
    from_delegate, 
    delegation_type, 
    eth_value, 
    gas_price, 
    gas_used, 
    gas_limit, 
    tx_fee
FROM 
    {{ ref('silver__delegations') }}