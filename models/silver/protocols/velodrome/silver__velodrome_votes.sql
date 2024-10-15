{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['stale']
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
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS token_id,
        (
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) / pow(
                10,
                18
            )
        ) AS vote_amount,
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
        )
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
        pool_address
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
        votes_base.contract_address AS contract_address,
        from_address,
        token_id :: INTEGER AS token_id,
        vote_amount,
        vote_action,
        _log_id,
        _inserted_timestamp,
        COALESCE(
            g1.pool_address,
            g0.pool_address
        ) AS pool_address,
        votes_base.event_index AS event_index
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
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_address,
    from_address,
    token_id,
    vote_amount,
    vote_action,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS votes_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
WHERE
    pool_address IS NOT NULL qualify(ROW_NUMBER() over(PARTITION BY tx_hash, pool_address, vote_action
ORDER BY
    _inserted_timestamp DESC) = 1)
