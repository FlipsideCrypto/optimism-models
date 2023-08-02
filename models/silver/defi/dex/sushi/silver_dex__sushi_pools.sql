{{ config(
    materialized = 'incremental',
    unique_key = "pool_address",
    tags = ['non_realtime']
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS pool_address,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS token0,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS token1,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address IN ('0xcaabdd9cf4b61813d4a52f980d6bc1b713fe66f5','0x1b02da8cb0d097eb8d57a175b88c7d8b47997506')
        AND topics [0] :: STRING = '0xe469f9471ac1d98222517eb2cdff1ef4df5f7880269173bb782bb78e499d9de3' --DeployPool

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    token0,
    token1,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation
