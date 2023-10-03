{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "{{ fsc_utils.block_reorg(this, 12) }}",
    tags = ['non_realtime']
) }}

WITH new_locks AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS provider_address,
        CASE
            WHEN topics [0] :: STRING = '0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624' THEN TO_TIMESTAMP(utils.udf_hex_to_int(topics [2] :: STRING))
            WHEN topics [0] :: STRING = '0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94' THEN TO_TIMESTAMP(
                utils.udf_hex_to_int(
                    segmented_data [2] :: STRING
                )
            )
        END AS unlock_date,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        (
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT / pow(
                10,
                18
            )
        ) :: FLOAT AS velo_value,
        CASE
            WHEN topics [0] :: STRING = '0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624' THEN utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        END AS deposit_type,
        CASE
            WHEN topics [0] :: STRING = '0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624' THEN 'deposit'
            WHEN topics [0] :: STRING = '0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94' THEN 'withdraw'
        END AS velo_action,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624',
            '0x02f25270a4d87bea75db541cdfe559334a275b4a233520ed6c0a2429667cca94'
        ) -- velo locks / unlocks
        AND contract_address = '0x9c7305eb78a432ced5c4d14cac27e8ed569a2e26'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    provider_address,
    unlock_date,
    velo_action,
    token_id :: NUMBER AS token_id,
    velo_value AS velo_amount,
    deposit_type,
    _log_id,
    _inserted_timestamp
FROM
    new_locks qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
