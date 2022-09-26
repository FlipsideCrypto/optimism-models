{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['velodrome']
) }}

WITH votes_base AS (

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS from_address,
        PUBLIC.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        (
            PUBLIC.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) / pow(
                10,
                18
            )
        ) AS amount,
        CASE
            WHEN topics [0] :: STRING = '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568' THEN 'unvote'
            WHEN topics [0] :: STRING = '0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15' THEN 'vote'
        END AS vote_action,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568',
            '0x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15'
        ) -- vote deposit / withdrawals
        AND origin_to_address = '0x09236cff45047dbee6b921e00704bed6d6b8cf7e'
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
gauges AS (
    SELECT
        gauge_address,
        external_bribe_address,
        internal_bribe_address,
        pool_address,
        pool_name
    FROM
        {{ ref('silver__velodrome_gauges') }}
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        COALESCE(
            g1.gauge_address,
            g0.gauge_address
        ) AS gauge_address,
        COALESCE(
            g1.external_bribe_address,
            g0.external_bribe_address
        ) AS external_bribe_address,
        COALESCE(
            g1.internal_bribe_address,
            g0.internal_bribe_address
        ) AS internal_bribe_address,
        votes_base.contract_address AS contract_address,
        COALESCE(
            g1.pool_address,
            g0.pool_address
        ) AS pool_address,
        COALESCE(
            g1.pool_name,
            g0.pool_name
        ) AS pool_name,
        from_address,
        token_id :: INTEGER AS token_id,
        amount AS vote_amount,
        vote_action,
        _log_id,
        _inserted_timestamp
    FROM
        votes_base
        LEFT JOIN gauges g1
        ON LOWER(
            votes_base.contract_address
        ) = LOWER(
            g1.external_bribe_address
        )
        LEFT JOIN gauges g0
        ON LOWER(
            votes_base.contract_address
        ) = LOWER(
            g0.internal_bribe_address
        )
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    gauge_address,
    external_bribe_address,
    internal_bribe_address,
    pool_address,
    pool_name,
    from_address,
    token_id,
    vote_amount,
    vote_action,
    _log_id,
    _inserted_timestamp
FROM
    FINAL
WHERE
    pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_hash, pool_address, vote_action
ORDER BY
    _inserted_timestamp DESC) = 1)
