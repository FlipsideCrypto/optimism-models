{{ config(
    materialized = 'incremental',
    unique_key = '_log _id',
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
    'hop_bridge_transfers_sent' AS NAME,
    event_index,
    topics [0] :: STRING AS topic_0,
    event_name,
    decoded_flat :"amount" :: INTEGER AS amount,
    decoded_flat :"amountOutMin" :: INTEGER AS amountOutMin,
    decoded_flat :"bonderFee" :: INTEGER AS bonderFee,
    decoded_flat :"chainId" :: INTEGER AS chainId,
    decoded_flat :"deadline" :: INTEGER AS deadline,
    decoded_flat :"index" :: INTEGER AS INDEX,
    decoded_flat :"recipient" :: STRING AS recipient,
    decoded_flat :"transferId" :: STRING AS transferId,
    decoded_flat :"transferNonce" :: STRING AS transferNonce,
    decoded_flat,
    DATA,
    event_removed,
    tx_status,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__decoded_logs') }}
WHERE
    topics [0] :: STRING = '0xe35dddd4ea75d7e9b3fe93af4f4e40e778c3da4074c9d93e7c6536f1e803c1eb'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
