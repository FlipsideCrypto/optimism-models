
    {{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    tags = ['non_realtime']
    ) }}
    
    SELECT 
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'multichain' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"_fallback"::STRING AS _fallback, decoded_flat:"appID"::STRING AS appID, decoded_flat:"data"::STRING AS data, decoded_flat:"flags"::INTEGER AS flags, decoded_flat:"from"::STRING AS from_address, decoded_flat:"nonce"::INTEGER AS nonce, decoded_flat:"to"::STRING AS to_address, decoded_flat:"toChainID"::INTEGER AS toChainID,
        decoded_flat,
        data,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        topics[0] :: STRING = '0xa17aef042e1a5dd2b8e68f0d0d92f9a6a0b35dc25be1d12c0cb3135bfd8951c9'
        AND contract_address = '0xc10ef9f491c9b59f936957026020c321651ac078'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    