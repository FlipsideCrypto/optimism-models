{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
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
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS amount0_in_unadj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS amount1_in_unadj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: FLOAT
        END AS amount0_out_unadj,
        CASE
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: FLOAT
        END AS amount1_out_unadj,
        CASE
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS fees0_adj,
        CASE
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS fees1_adj,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS transfer_amount,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        event_index,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN 'Transfer'
            WHEN topics [0] :: STRING = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602' THEN 'Fees'
            WHEN topics [0] :: STRING = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' THEN 'Swap'
        END AS event_name,
        'velodrome' AS platform,
        topics [0] :: STRING AS function_type
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
            '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602',
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
        )
        AND tx_succeeded
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
),
transfers AS (
    SELECT
        tx_hash,
        contract_address AS fee_currency,
        transfer_amount AS fee_amount
    FROM
        base
    WHERE
        function_type = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
),
lp_fees AS (
    SELECT
        A.tx_hash AS tx_hash,
        A.contract_address AS contract_address,
        fees0_adj,
        fees1_adj,
        CASE
            WHEN fees0_adj = 0 THEN fees1_adj :: FLOAT
            WHEN fees1_adj = 0 THEN fees0_adj :: FLOAT
        END AS fees_adj,
        fee_currency,
        ROW_NUMBER() over (
            PARTITION BY A.tx_hash,
            contract_address
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        base A
        LEFT JOIN transfers b
        ON A.tx_hash = b.tx_hash
        AND (
            CASE
                WHEN fees0_adj = 0 THEN fees1_adj :: FLOAT
                WHEN fees1_adj = 0 THEN fees0_adj :: FLOAT
            END
        ) = b.fee_amount
    WHERE
        function_type = '0x112c256902bf554b6ed882d2936687aaeb4225e8cd5b51303c90ca6cf43a8602'
),
swaps AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        sender_address,
        to_address,
        amount0_in_unadj,
        amount1_in_unadj,
        amount0_out_unadj,
        amount1_out_unadj,
        _log_id,
        _inserted_timestamp,
        event_index,
        event_name,
        platform,
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            contract_address
            ORDER BY
                event_index ASC
        ) AS agg_id
    FROM
        base
    WHERE
        function_type = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' qualify(ROW_NUMBER() over(PARTITION BY _log_id
    ORDER BY
        _inserted_timestamp DESC) = 1)
)
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
    amount0_in_unadj,
    amount1_in_unadj,
    amount0_out_unadj,
    amount1_out_unadj,
    _log_id,
    _inserted_timestamp,
    event_index,
    event_name,
    platform,
    COALESCE(
        fees_adj,
        0
    ) AS lp_fee_unadj,
    fee_currency,
    {{ dbt_utils.generate_surrogate_key(
        ['b.tx_hash', 'event_index']
    ) }} AS swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    swaps b
    LEFT JOIN lp_fees l
    ON b.contract_address = l.contract_address
    AND b.tx_hash = l.tx_hash
    AND b.agg_id = l.agg_id
