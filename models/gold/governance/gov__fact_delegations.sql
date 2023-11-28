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
    event_name,
    delegator, 
    to_delegate, 
    from_delegate, 
    delegation_type,
    new_balance,
    previous_balance,
    raw_new_balance, 
    raw_previous_balance,	    
    COALESCE (
        delegations_id,
        {{ dbt_utils.generate_surrogate_key(
            ['_log_id']
        ) }}
    ) AS fact_delegations_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM 
    {{ ref('silver__delegations') }}