{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH pools AS (

    SELECT
        pool_address,
        token0,
        token1
    FROM
        {{ ref('silver_dex__sushi_pools') }}
),
swaps_base AS (
    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amountOut,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS tx_to,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tokenIn,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS tokenOut,
        token0,
        token1,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        INNER JOIN pools p
        ON p.pool_address = contract_address
    WHERE
        topics [0] :: STRING = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062'
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    origin_from_address AS sender,
    tx_to,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    tokenIn AS token_in,
    tokenOut AS token_out,
    token0,
    token1,
    'Swap' AS event_name,
    'sushiswap' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base