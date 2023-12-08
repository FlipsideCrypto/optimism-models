{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'hop' AS NAME,
        event_index,
        topics [0] :: STRING AS topic_0,
        event_name,
        TRY_TO_NUMBER(
            decoded_flat :"amount" :: STRING
        ) AS amount,
        TRY_TO_NUMBER(
            decoded_flat :"amountOutMin" :: STRING
        ) AS amountOutMin,
        TRY_TO_NUMBER(
            decoded_flat :"bonderFee" :: STRING
        ) AS bonderFee,
        TRY_TO_NUMBER(
            decoded_flat :"chainId" :: STRING
        ) AS chainId,
        TRY_TO_TIMESTAMP(
            decoded_flat :"deadline" :: STRING
        ) AS deadline,
        TRY_TO_TIMESTAMP(
            decoded_flat :"index" :: STRING
        ) AS index,
        decoded_flat :"recipient" :: STRING AS recipient,
        decoded_flat :"transferId" :: STRING AS transferId,
        decoded_flat :"transferNonce" :: STRING AS transferNonce,
        decoded_flat,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0xe35dddd4ea75d7e9b3fe93af4f4e40e778c3da4074c9d93e7c6536f1e803c1eb'
        AND origin_to_address IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
hop_tokens AS (
    SELECT
        block_number,
        contract_address,
        amm_wrapper_address,
        token_address,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__hop_l2canonicaltoken') }}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    topic_0,
    event_name,
    event_removed,
    tx_status,
    contract_address AS bridge_address,
    amm_wrapper_address, 
    NAME AS platform,
    origin_from_address AS sender,
    recipient AS receiver,
    receiver AS destination_chain_receiver,
    chainId AS destination_chain_id,
    token_address,
    amount,
    amountOutMin AS amount_out_min,
    bonderFee AS bonder_fee,
    deadline,
    index,
    transferId AS transfer_id,
    transferNonce AS transfer_nonce,   
    _log_id,
    _inserted_timestamp
FROM
    base_evt b
    LEFT JOIN hop_tokens h USING(contract_address)
