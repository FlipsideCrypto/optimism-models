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
    'synapse' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"amount" :: INTEGER AS amount,
    decoded_flat :"chainId" :: INTEGER AS chainId,
    decoded_flat :"deadline" :: INTEGER AS deadline,
    decoded_flat :"minDy" :: INTEGER AS minDy,
    decoded_flat :"to" :: STRING AS to_address,
    decoded_flat :"token" :: STRING AS token,
    decoded_flat :"tokenIndexFrom" :: INTEGER AS tokenIndexFrom,
    decoded_flat :"tokenIndexTo" :: INTEGER AS tokenIndexTo,
    decoded_flat,
    DATA,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0x91f25e9be0134ec851830e0e76dc71e06f9dade75a9b84e9524071dbbc319425'
    AND contract_address IN ('0xaf41a65f786339e7911f4acdad6bd49426f2dc6b')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
