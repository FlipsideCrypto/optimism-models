{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address AS factory_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    LOWER(CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40))) AS token0,
    LOWER(CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))) AS token1,
    CASE
        WHEN RIGHT(
            topics [3] :: STRING,
            1
        ) = '0' THEN FALSE
        ELSE TRUE
    END AS stable,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
    CONCAT(
        tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    modified_timestamp AS _inserted_timestamp
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    topics [0] = '0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e'
    AND contract_address = '0xf1046053aa5682b4f9a81b5481394da16be5ff5a'
    AND pool_address <> '0x585af0b397ac42dbef7f18395426bf878634f18d' -- velo v1/v2 converter
    AND block_number > 100000000
    AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
