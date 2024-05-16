{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']

) }}

WITH router_swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS from_token,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_token,
        CONCAT('0x', SUBSTR(l.topics [3] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS swapType,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address IN (
            '0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024' --v2
        )
        AND topics [0] :: STRING = '0x27c98e911efdd224f4002f6cd831c3ad0d2759ee176f9ee8466d95826af22a1c' --WooRouterSwap
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
swaps_base AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS from_token,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_token,
        CONCAT('0x', SUBSTR(l.topics [3] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [2] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address = '0xeaf1ac8e89ea0ae13e0f03634a4ff23502527024' --v2
        AND topics [0] :: STRING = '0x0e8e403c2d36126272b08c75823e988381d9dc47f2f0a9a080d95f891d95c469' --WooSwap
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                router_swaps_base
        )
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
    event_index,
    contract_address,
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4200000000000000000000000000000000000006'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4200000000000000000000000000000000000006'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    swapType AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooRouterSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    router_swaps_base
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4200000000000000000000000000000000000006'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x4200000000000000000000000000000000000006'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    NULL AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
