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
    raw_new_balance, 
    raw_previous_balance
FROM 
    {{ ref('silver__delegations') }}