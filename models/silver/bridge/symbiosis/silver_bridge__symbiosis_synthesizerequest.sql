
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
        'symbiosis' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"amount"::INTEGER AS amount, decoded_flat:"chainID"::INTEGER AS chainID, decoded_flat:"from"::STRING AS from_address, decoded_flat:"id"::STRING AS id, decoded_flat:"revertableAddress"::STRING AS revertableAddress, decoded_flat:"to"::STRING AS to_address, decoded_flat:"token"::STRING AS token,
        decoded_flat,
        data,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        topics[0] :: STRING = '0x31325fe0a1a2e6a5b1e41572156ba5b4e94f0fae7e7f63ec21e9b5ce1e4b3eab'
        AND contract_address = '0x292fc50e4eb66c3f6514b9e402dbc25961824d62'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    