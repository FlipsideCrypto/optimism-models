{{ config(
    materialized = 'incremental',
    unique_key = "pool_address"
) }}

WITH contract_deployments AS (

SELECT
    tx_hash,
    block_timestamp,
    contract_address AS deployer_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x',SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS underlying_token,
    CONCAT('0x',SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS pool_address,
    _inserted_timestamp
FROM {{ ref('silver__logs') }}
WHERE topics[0] = '0x3042e0d13ca8ee0bdf1dc720371ee18ade19c59a8dfc8db85869f433cedaf015'
    AND origin_to_address IN ('0x203328c161d23dceee3e439deeb25ca19e2c4984')
    AND tx_status = 'SUCCESS'
{% if is_incremental() %}
AND pool_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
)

SELECT
    deployer_address,
    underlying_token AS underlying_token_address,
    pool_address,
    token_symbol AS underlying_token_symbol,
    CASE
        WHEN token_symbol = 'WETH' THEN 'bathETH'
        ELSE CONCAT('bath',token_symbol) 
    END AS pool_name,
    d._inserted_timestamp
FROM contract_deployments d
LEFT JOIN {{ ref('silver__contracts') }} c 
    ON d.underlying_token = c.contract_address