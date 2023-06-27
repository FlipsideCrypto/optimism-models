{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['velodrome']
) }}

WITH lp_actions AS (

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
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))
        END AS sender_address,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT
        END AS amount0_unadj,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT
        END AS amount1_unadj,
        CASE
            WHEN topics [0] :: STRING IN(
                '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
                '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
            ) THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
        END AS to_address,
        CASE
            WHEN topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' THEN (
                utils.udf_hex_to_int(
                    segmented_data [0] :: STRING
                ) :: FLOAT / pow(
                    10,
                    18
                )
            )
        END AS lp_token_amount,
        CASE
            WHEN topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' THEN 'deposit'
            WHEN topics [0] :: STRING = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' THEN 'withdraw'
        END AS lp_action,
        CASE
            WHEN topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' THEN 'mint'
            WHEN topics [0] :: STRING = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' THEN 'burn'
        END AS lp_token_action,
        topics [0] :: STRING AS function_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (
            topics [0] :: STRING = '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f' -- deposits
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- lp mints
                AND topics [1] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000'
            )
            OR topics [0] :: STRING = '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496' -- withdrawls
            OR (
                topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef' -- lp burns
                AND topics [2] :: STRING = '0x0000000000000000000000000000000000000000000000000000000000000000'
            )
        )
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
),
lp_tokens_actions AS (
    SELECT
        tx_hash,
        contract_address,
        lp_token_amount
    FROM
        lp_actions
    WHERE
        function_type = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
)
SELECT
    block_number,
    block_timestamp,
    A.tx_hash AS tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    A.contract_address AS contract_address,
    sender_address,
    amount0_unadj,
    amount1_unadj,
    lp_action,
    lp_token_action,
    function_type,
    _log_id,
    _inserted_timestamp,
    b.lp_token_amount AS lp_token_amount
FROM
    lp_actions A
    LEFT JOIN lp_tokens_actions b
    ON A.tx_hash = b.tx_hash
    AND A.contract_address = b.contract_address
WHERE
    function_type IN(
        '0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496',
        '0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f'
    ) qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
