{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['curated']
) }}

WITH contract_deployments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        origin_from_address AS deployer_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = LOWER('0x6D551f4D999faC0984eb75B2B230ba7e7651BdE7') --does not look like any pools have been created as of 10/26/23
        AND topics [0] :: STRING = '0xdbd2a1ea6808362e6adbec4db4969cbc11e3b0b28fb6c74cb342defaaf1daada'

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
    tx_hash,
    block_number,
    block_timestamp,
    deployer_address,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    contract_deployments
