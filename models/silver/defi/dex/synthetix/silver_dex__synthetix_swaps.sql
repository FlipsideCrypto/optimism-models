{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH swaps_base AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'Swap' AS event_name,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS sender,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS amount_in_unadj,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS amount_out_unadj,
        REGEXP_REPLACE(
            utils.udf_hex_to_string(
                segmented_data [0] :: STRING
            ),
            '[^a-zA-Z0-9]+'
        ) AS symbol_in,
        REGEXP_REPLACE(
            utils.udf_hex_to_string(
                segmented_data [2] :: STRING
            ),
            '[^a-zA-Z0-9]+'
        ) AS symbol_out,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS tx_to,
        event_index,
        'synthetix' AS platform,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            '0x8700daec35af8ff88c16bdf0418774cb3d7599b4'
        )
        AND topics [0] = '0x65b6972c94204d84cffd3a95615743e31270f04fdf251f3dccc705cfbad44776'
        AND tx_succeeded

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
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    sender,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    tx_to,
    event_index,
    platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base s
    LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_in,
            synth_proxy_address AS token_in,
            decimals AS decimals_in
        FROM
            {{ ref('silver__synthetix_synths_20230404') }}
    ) sc1
    ON sc1.synth_symbol_in = s.symbol_in
    LEFT JOIN (
        SELECT
            synth_symbol AS synth_symbol_out,
            synth_proxy_address AS token_out,
            decimals AS decimals_out
        FROM
            {{ ref('silver__synthetix_synths_20230404') }}
    ) sc2
    ON sc2.synth_symbol_out = s.symbol_out
