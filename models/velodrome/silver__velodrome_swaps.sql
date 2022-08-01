{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['velodrome']
) }}

WITH base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS sender_address,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS to_address,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS amount0_in_adj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS amount1_in_adj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: FLOAT
        END AS amount0_out_adj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN PUBLIC.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: FLOAT
        END AS amount1_out_adj,
        CASE
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN PUBLIC.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS fees0_adj,
        CASE
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS fees1_adj,
        _log_id,
        _inserted_timestamp,
        event_index,
        'velodrome' AS platform,
        topics [0] :: STRING AS function_type
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602'
        )
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
lp_fees AS (
    SELECT
        tx_hash,
        contract_address,
        fees0_adj,
        fees1_adj,
        CASE
            WHEN fees0_adj = 0 THEN fees1_adj
            WHEN fees1_adj = 0 THEN fees0_adj
        END AS fees_adj
    FROM
        base
    WHERE
        function_type = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602'
),
velo_pools AS (
    SELECT
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals
    FROM
        {{ ref('silver__velodrome_pools') }}
),
combine_meta AS (
    SELECT
        block_number,
        block_timestamp,
        b.tx_hash AS tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        b.contract_address AS contract_address,
        sender_address,
        to_address,
        amount0_in_adj,
        amount0_out_adj,
        amount1_in_adj,
        amount1_out_adj,
        _log_id,
        _inserted_timestamp,
        event_index,
        platform,
        pool_address,
        pool_name,
        pool_type,
        token0_symbol,
        token1_symbol,
        token0_address,
        token1_address,
        token0_decimals,
        token1_decimals,
        CASE
            WHEN amount0_in_adj <> 0 THEN token0_decimals
            WHEN amount1_in_adj <> 0 THEN token1_decimals
        END AS decimals_in,
        CASE
            WHEN amount0_out_adj <> 0 THEN token0_decimals
            WHEN amount1_out_adj <> 0 THEN token1_decimals
        END AS decimals_out,
        CASE
            WHEN amount0_in_adj <> 0 THEN amount0_in_adj
            WHEN amount1_in_adj <> 0 THEN amount1_in_adj
        END AS amount_in_adj,
        CASE
            WHEN amount0_out_adj <> 0 THEN amount0_out_adj
            WHEN amount1_out_adj <> 0 THEN amount1_out_adj
        END AS amount_out_adj,
        CASE
            WHEN decimals_in IS NOT NULL THEN amount_in_adj / pow(
                10,
                decimals_in
            )
            ELSE amount_in_adj
        END AS amount_in,
        CASE
            WHEN decimals_out IS NOT NULL THEN amount_out_adj / pow(
                10,
                decimals_out
            )
            ELSE amount_out_adj
        END AS amount_out,
        CASE
            WHEN amount0_in_adj <> 0 THEN token0_address
            WHEN amount1_in_adj <> 0 THEN token1_address
        END AS token_address_in,
        CASE
            WHEN amount0_out_adj <> 0 THEN token0_address
            WHEN amount1_out_adj <> 0 THEN token1_address
        END AS token_address_out,
        CASE
            WHEN amount0_in_adj <> 0 THEN token0_symbol
            WHEN amount1_in_adj <> 0 THEN token1_symbol
        END AS symbol_in,
        CASE
            WHEN amount0_out_adj <> 0 THEN token0_symbol
            WHEN amount1_out_adj <> 0 THEN token1_symbol
        END AS symbol_out,
        CASE
            WHEN decimals_in IS NOT NULL THEN fees_adj / pow(
                10,
                decimals_in
            )
            ELSE fees_adj
        END AS lp_fee
    FROM
        base b
        INNER JOIN velo_pools
        ON b.contract_address = pool_address
        LEFT JOIN lp_fees l
        ON b.contract_address = l.contract_address
        AND b.tx_hash = l.tx_hash
    WHERE
        function_type = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
),
token_prices AS (
    SELECT
        HOUR,
        token_address,
        price
    FROM
        {{ ref('silver__prices') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                combine_meta
        )
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    contract_address,
    pool_address,
    pool_name,
    amount_in,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            amount_in * p0.price,
            2
        )
        ELSE NULL
    END AS amount_in_usd,
    amount_out,
    CASE
        WHEN decimals_out IS NOT NULL THEN ROUND(
            amount_out * p1.price,
            2
        )
        ELSE NULL
    END AS amount_out_usd,
    sender_address,
    to_address,
    event_index,
    _log_id,
    platform,
    _inserted_timestamp,
    token_address_in,
    token_address_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    token0_decimals,
    token1_decimals,
    token0_symbol,
    token1_symbol,
    lp_fee,
    CASE
        WHEN decimals_in IS NOT NULL THEN ROUND(
            lp_fee * p0.price,
            2
        )
        ELSE NULL
    END AS lp_fee_usd,
    symbol_in AS lp_fee_symbol,
    token_address_in AS lp_fee_token_address
FROM
    combine_meta
    LEFT JOIN token_prices AS p0
    ON p0.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token_address_in = p0.token_address
    LEFT JOIN token_prices AS p1
    ON p1.hour = DATE_TRUNC(
        'hour',
        block_timestamp
    )
    AND token_address_out = p1.token_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
