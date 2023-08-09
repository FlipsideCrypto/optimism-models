
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
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        decoded_flat:"amount"::INTEGER AS "AMOUNT", decoded_flat:"amountOutMin"::INTEGER AS "AMOUNTOUTMIN", decoded_flat:"bonderFee"::INTEGER AS "BONDERFEE", decoded_flat:"chainId"::INTEGER AS "CHAINID", decoded_flat:"deadline"::INTEGER AS "DEADLINE", decoded_flat:"index"::INTEGER AS "INDEX", decoded_flat:"recipient"::STRING AS "RECIPIENT", decoded_flat:"transferId"::STRING AS "TRANSFERID", decoded_flat:"transferNonce"::STRING AS "TRANSFERNONCE",
        decoded_flat,
        data,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        --contract_address = '0x3666f603cc164936c1b87e207f36beba4ac5f18a'
        --AND 
        topics[0] :: STRING = '0xe35dddd4ea75d7e9b3fe93af4f4e40e778c3da4074c9d93e7c6536f1e803c1eb'
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{ this }}
    )
    {% endif %}
    LIMIT 10
    