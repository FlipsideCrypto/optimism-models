
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
        'synapse' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"amount"::INTEGER AS amount, decoded_flat:"chainId"::INTEGER AS chainId, decoded_flat:"swapDeadline"::INTEGER AS swapDeadline, decoded_flat:"swapMinAmount"::INTEGER AS swapMinAmount, decoded_flat:"swapTokenIndex"::INTEGER AS swapTokenIndex, decoded_flat:"to"::STRING AS to_address, decoded_flat:"token"::STRING AS token,
        decoded_flat,
        data,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        topics[0] :: STRING = '0x9a7024cde1920aa50cdde09ca396229e8c4d530d5cfdc6233590def70a94408c'
        AND contract_address = '0xaf41a65f786339e7911f4acdad6bd49426f2dc6b'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    