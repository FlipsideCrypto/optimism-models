{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH asset_details AS (

    SELECT
        borrowable1 AS token_address,
        token0 AS underlying_asset_address,
        token0_name AS underlying_asset_name,
        token0_symbol AS underlying_asset_symbol,
        token0_decimals AS underlying_decimals
    FROM
        {{ ref('silver__tarot_liquidity_pools') }}
    GROUP BY
        ALL
    UNION ALL
    SELECT
        borrowable2 AS token_address,
        token1 AS underlying_asset_address,
        token1_name AS underlying_asset_name,
        token1_symbol AS underlying_asset_symbol,
        token1_decimals AS underlying_decimals
    FROM
        {{ ref('silver__tarot_liquidity_pools') }}
    GROUP BY
        ALL
),
log_pull AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 25, 40)) AS borrower,
        TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        )) AS loan_amount_raw,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
        l
    WHERE
        contract_address IN (
            SELECT
                token_address
            FROM
                asset_details
        )
        AND topics [0] :: STRING = '0x33f3048bd4e6af45e53afb722adfd57dbde82da7e93e44db921fb4b8c6a70c4b'
        AND tx_succeeded
        AND loan_amount_raw > 0 --borrow and repay in same log event, value in segmented data determines if borrow or repay

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    underlying_asset_address AS borrows_contract_address,
    underlying_asset_symbol AS borrows_contract_symbol,
    a.token_address AS token_address,
    'bTAROT' AS token_symbol,
    loan_amount_raw AS amount_unadj,
    loan_amount_raw / pow(
        10,
        underlying_decimals
    ) AS amount,
    'Tarot' AS platform,
    _inserted_timestamp,
    _log_id
FROM
    log_pull l
    LEFT JOIN asset_details A
    ON A.token_address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
