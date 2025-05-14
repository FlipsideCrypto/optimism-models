{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['gold','governance','curated']
) }}

SELECT 
    block_number, 
    block_timestamp, 
    tx_hash, 
    status, 
    event_name,
    delegator, 
    delegate,
    delegation_type,
    new_balance,
    previous_balance,
    raw_new_balance, 
    raw_previous_balance,	
    to_delegate, 
    from_delegate,     
    delegations_id AS fact_delegations_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__delegations') }}