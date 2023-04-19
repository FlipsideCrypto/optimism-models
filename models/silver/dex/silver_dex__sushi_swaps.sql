{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'SUSHI',
    'PURPOSE': 'DEFI, DEX, SWAPS' }} }
) }}

WITH swap_events AS (

    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: INTEGER
        ) AS amountIn,
        TRY_TO_NUMBER(
            ethereum.public.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
        ) AS amountOut,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_out,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_in,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS tx_to,
        event_index,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062'
        AND tx_status = 'SUCCESS'
        AND contract_address IN (
            SELECT
                DISTINCT pool_address
            FROM
                {{ ref('silver_dex__sushi_pools') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        tx_hash,
        contract_address,
        event_name,
        token0_decimals,
        token1_decimals,
        CASE
            WHEN token_in = token0_address THEN amountIn :: FLOAT
            WHEN token_in = token1_address THEN amountIn :: FLOAT
        END AS amount_in_unadj,
        CASE
            WHEN token_in = token0_address THEN amountIn / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN token_in = token1_address THEN amountIn / power(
                10,
                token1_decimals
            ) :: FLOAT
        END AS amount_in,
        CASE
            WHEN token_out = token0_address THEN amountOut :: FLOAT
            WHEN token_out = token1_address THEN amountOut :: FLOAT
        END AS amount_out_unadj,
        CASE
            WHEN token_out = token0_address THEN amountOut / power(
                10,
                token0_decimals
            ) :: FLOAT
            WHEN token_out = token1_address THEN amountOut / power(
                10,
                token1_decimals
            ) :: FLOAT
        END AS amount_out,
        tx_to,
        event_index,
        _log_id,
        CASE
            WHEN token_in = token0_address THEN token0_symbol
            WHEN token_in = token1_address THEN token1_symbol
        END AS symbol_in,
        CASE
            WHEN token_out = token0_address THEN token0_symbol
            WHEN token_out = token1_address THEN token1_symbol
        END AS symbol_out,
        token_in,
        token_out,
        pool_name,
        _inserted_timestamp
    FROM
        swap_events A
        LEFT JOIN {{ ref('silver_dex__sushi_pools') }}
        bb
        ON A.contract_address = bb.pool_address
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    'sushiswap' AS platform,
    pool_name,
    amount_in_unadj,
    amount_in,
    amount_out_unadj,
    amount_out,
    tx_to,
    event_index,
    event_name,
    token_in,
    token_out,
    token0_decimals AS decimals_in,
    token1_decimals AS decimals_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
FROM
    FINAL
