{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
) }}

WITH staking_actions AS (

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
            WHEN topics [0] :: STRING = '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7' THEN 'deposit'
            WHEN topics [0] :: STRING = '0xf341246adaac6f497bc2a656f546ab9e182111d630394f0c57c710a59a2cb567' THEN 'withdraw'
        END AS staking_action_type,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS token_id,
        (
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: FLOAT / pow(
                10,
                18
            )
        ) :: FLOAT AS amount,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS lp_provider_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS gauge_address,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7',
            '0xf341246adaac6f497bc2a656f546ab9e182111d630394f0c57c710a59a2cb567'
        ) -- deposit / withdrawal
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
token_transfer AS (
    SELECT
        tx_hash,
        event_index,
        contract_address AS pool_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS gauge_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lp_provider_address,
        (
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: FLOAT / pow(
                10,
                18
            )
        ) :: FLOAT AS amount
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        block_timestamp IN (
            SELECT
                DISTINCT block_timestamp
            FROM
                staking_actions
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                staking_actions
        )
        AND topics [0] :: STRING = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'

{% if is_incremental() %}
AND modified_timestamp >= (
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
    A.tx_hash AS tx_hash,
    origin_function_signature,
    origin_from_address,
    CASE
        WHEN origin_to_address IS NULL THEN contract_address
        ELSE origin_to_address
    END AS origin_to_address,
    contract_address,
    A.event_index AS event_index,
    staking_action_type,
    A.amount AS lp_token_amount,
    A.lp_provider_address AS lp_provider_address,
    A.gauge_address AS gauge_address,
    b.pool_address AS pool_address,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['a.tx_hash', 'a.event_index']
    ) }} AS locks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    staking_actions A
    LEFT JOIN token_transfer b
    ON A.tx_hash = b.tx_hash
    AND A.amount = b.amount
WHERE
    b.pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
