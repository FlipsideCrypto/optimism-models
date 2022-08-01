{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['velodrome']
) }}

WITH velo_distributions AS (

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
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS token_id,
        PUBLIC.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: FLOAT / pow(
            10,
            18
        ) :: FLOAT AS amount,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS claim_epoch,
        PUBLIC.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS max_epoch,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xcae2990aa9af8eb1c64713b7eddb3a80bf18e49a94a13fe0d0002b5d61d58f00'
        AND contract_address = '0x5d5bea9f0fc13d967511668a60a3369fd53f784f'
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
                velo_distributions
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
    token_id,
    amount AS claimed_velo,
    ROUND(
        velo_price * claimed_velo,
        2
    ) AS claimed_velo_amount_usd,
    claim_epoch,
    max_epoch,
    _log_id,
    _inserted_timestamp
FROM
    velo_distributions
    LEFT JOIN velo_price
    ON HOUR = DATE_TRUNC(
        'hour',
        block_timestamp
    ) qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
