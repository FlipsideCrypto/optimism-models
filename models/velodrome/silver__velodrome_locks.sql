{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS provider_address,
        TO_TIMESTAMP(PUBLIC.udf_hex_to_int(topics [2] :: STRING)) AS unlock_date,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        (
            PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT / pow(
                10,
                18
            )
        ) :: FLOAT AS velo_value,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS deposit_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xff04ccafc360e16b67d682d17bd9503c4c6b9a131f6be6325762dc9ffc7de624' -- velo locks
        AND contract_address = '0x9c7305eb78a432ced5c4d14cac27e8ed569a2e26'
        AND tx_status = 'SUCCESS'
        AND event_removed = 'false'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
velo_price AS (
    SELECT
        HOUR,
        price AS velo_price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                new_locks
        )
        AND symbol = 'VELO'
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
    token_id,
    velo_value,
    ROUND(
        velo_price * velo_value,
        2
    ) AS velo_value_usd,
    deposit_type,
    _log_id,
    _inserted_timestamp
FROM
    new_locks
    LEFT JOIN velo_price
    ON HOUR = DATE_TRUNC(
        'hour',
        block_timestamp
    ) qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
