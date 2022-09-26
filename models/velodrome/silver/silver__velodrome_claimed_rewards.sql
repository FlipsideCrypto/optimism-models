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
        ) :: FLOAT AS claimed_amount,
        PUBLIC.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INTEGER AS claim_epoch,
        PUBLIC.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS max_epoch,
        'venft_distribution' AS reward_type,
        _log_id,
        _inserted_timestamp,
        '0x3c8b650257cfb5f272f799f5e2b4e65093a11a05' AS token_address
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
        )
    FROM
        {{ this }}
)
{% endif %}
),
staking_rewards AS (
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
        ) :: INTEGER AS claimed_amount,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS reward_token,
        CASE
            WHEN origin_to_address = '0x6b8edc43de878fd5cd5113c42747d32500db3873' THEN 'lp_reward'
            ELSE 'voter_reward'
        END AS reward_type,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x9aa05b3d70a9e3e2f004f039648839560576334fb45c81f91b6db03ad9e2efc9'
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
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        reward_type,
        token_id,
        claimed_amount,
        token_address,
        claim_epoch,
        max_epoch,
        _log_id,
        _inserted_timestamp
    FROM
        velo_distributions
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        reward_type,
        NULL AS token_id,
        claimed_amount,
        reward_token AS token_address,
        NULL AS claim_epoch,
        NULL AS max_epoch,
        _log_id,
        _inserted_timestamp
    FROM
        staking_rewards
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
    reward_type,
    token_id,
    claimed_amount,
    token_address,
    claim_epoch,
    max_epoch,
    _log_id,
    _inserted_timestamp
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC) = 1)
