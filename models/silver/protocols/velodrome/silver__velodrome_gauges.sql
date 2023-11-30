{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    tags = ['curated','reorg']
) }}

WITH backfill AS (

    SELECT
        LOWER(gauge_address) AS gauge_address,
        LOWER(creator_address) AS creator_address,
        LOWER(internal_bribe_address) AS internal_bribe_address,
        LOWER(external_bribe_address) AS external_bribe_address,
        LOWER(pool_address) AS pool_address,
        LOWER(contract_address) AS contract_address,
        tx_hash,
        event_index,
        block_timestamp :: TIMESTAMP AS block_timestamp,
        block_number,
        '1970-01-01' :: DATE AS _inserted_timestamp
    FROM
        {{ ref('silver__velo_gauges_backfill') }}
),
new_gauges AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS gauge_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS external_bribe_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS pool_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) AS creator_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) AS internal_bribe_address,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xa4d97e9e7c65249b4cd01acb82add613adea98af32daf092366982f0a0d4e453'
        AND contract_address = '0x09236cff45047dbee6b921e00704bed6d6b8cf7e'

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
all_gauges AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        event_index,
        contract_address,
        gauge_address,
        external_bribe_address,
        internal_bribe_address,
        pool_address,
        creator_address,
        _inserted_timestamp
    FROM
        backfill
    UNION ALL
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        event_index,
        contract_address,
        gauge_address,
        external_bribe_address,
        internal_bribe_address,
        pool_address,
        creator_address,
        _inserted_timestamp
    FROM
        new_gauges
)
SELECT
    block_timestamp,
    block_number,
    tx_hash,
    event_index,
    contract_address,
    gauge_address,
    external_bribe_address,
    internal_bribe_address,
    pool_address,
    creator_address,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS gauges_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_gauges qualify(ROW_NUMBER() over(PARTITION BY gauge_address
ORDER BY
    _inserted_timestamp DESC) = 1)
