{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH log_pull AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(tx_hash :: STRING, '-', event_index :: STRING) AS _log_id
    FROM {{ ref('core__fact_event_logs') }}
    WHERE topics[0] :: STRING = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
      AND origin_from_address = LOWER('0xFb59Ce8986943163F14C590755b29dB2998F2322')
    {% if is_incremental() %}
      AND _inserted_timestamp >= (
        SELECT MAX(_inserted_timestamp) - INTERVAL '12 hours' FROM {{ this }}
      )
      AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
),
traces_pull AS (
    SELECT
        from_address AS token_address,
        to_address AS underlying_asset
    FROM {{ ref('core__fact_traces') }}
    WHERE tx_hash IN (
            SELECT tx_hash FROM log_pull
        )
      AND concat_ws('_', TYPE, trace_address) = 'STATICCALL_2'
),
contracts AS (
    SELECT * FROM {{ ref('silver__contracts') }}
),
contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        t.underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM log_pull l
    LEFT JOIN traces_pull t ON l.contract_address = t.token_address
    LEFT JOIN contracts C ON C.contract_address = l.contract_address
    QUALIFY ROW_NUMBER() OVER (PARTITION BY l.contract_address ORDER BY block_timestamp ASC) = 1
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS token_address,
    l.token_name,
    l.token_symbol,
    l.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    l._inserted_timestamp,
    l._log_id
FROM contract_pull l
LEFT JOIN contracts C ON C.contract_address = l.underlying_asset
WHERE underlying_asset IS NOT NULL
  AND l.token_name IS NOT NULL
