{{ config(
    materialized = 'incremental',
    unique_key = "pool_address",
    tags = ['non_realtime']
) }}

WITH pool_tr AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address AS deployer_address,
        to_address AS pool_address,
        _call_id,
        _inserted_timestamp
    FROM
        {{ ref(
            'silver__traces'
        ) }}
    WHERE
        from_address IN (
            '0x386a28709a31532d4f68b06fd28a27e4ea378364'
        )
        AND TYPE ILIKE 'create%'
        AND tx_status ILIKE 'success'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
AND to_address NOT IN (
    SELECT
        DISTINCT pool_address
    FROM
        {{ this }}
)
{% endif %}
),
pool_evt AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS quoteToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS creator,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref ('silver__logs') }}
    WHERE
        contract_address IN (
            '0xdb9c53f2ced34875685b607c97a61a65da2f30a8',
            --dpp - factory,
            '0x1f83858cd6d0ae7a08ab1fd977c06dabece6d711',
            --dsp - factory
            '0x2b800dc6270726f7e2266ce8cd5a3f8436fe0b40' --dvm - factory
        )
        AND topics [0] :: STRING IN (
            '0x8494fe594cd5087021d4b11758a2bbc7be28a430e94f2b268d668e5991ed3b8a',
            --NewDPP
            '0xbc1083a2c1c5ef31e13fb436953d22b47880cf7db279c2c5666b16083afd6b9d',
            --NewDSP
            '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' --NewDVM
        )

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
),
FINAL AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        contract_address,
        baseToken AS base_token,
        quoteToken AS quote_token,
        creator,
        pool_address,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        pool_evt
    UNION
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        NULL AS event_index,
        deployer_address AS contract_address,
        NULL AS base_token,
        NULL AS quote_token,
        deployer_address AS creator,
        pool_address,
        _call_id AS _id,
        _inserted_timestamp
    FROM
        pool_tr
)
SELECT
    *
FROM
    FINAL qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
