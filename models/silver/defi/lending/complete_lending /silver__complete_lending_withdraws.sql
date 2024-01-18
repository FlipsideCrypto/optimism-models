{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curated']
) }}

WITH withdraws AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        depositor_address,
        'Aave V3' AS platform,
        'arbitrum' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    radiant_token AS protocol_market,
    radiant_market AS token_address,
    symbol AS token_symbol,
    amount_unadj,
    amount,
    depositor_address,
    platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__radiant_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    compound_market AS protocol_market,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    depositor_address,
    compound_version AS platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__comp_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    itoken AS protocol_market,
    received_contract_address AS token_address,
    received_contract_symbol token_symbol,
    amount_unadj,
    received_amount,
    redeemer AS depositor_address,
    platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__lodestar_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    token_address AS protocol_market,
    received_contract_address AS token_address,
    received_contract_symbol token_symbol,
    amount_unadj,
    amount,
    redeemer AS depositor_address,
    platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__dforce_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    silo_market AS protocol_market,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    depositor_address,
    platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__silo_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
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
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        A.contract_address,
        CASE
            WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
            WHEN platform = 'Lodestar' THEN 'Redeem'
            ELSE 'Withdraw'
        END AS event_name,
        protocol_market,
        depositor_address AS depositor,
        A.token_address,
        A.token_symbol,
        amount_unadj,
        amount,
        ROUND(
            amount * price,
            2
        ) AS amount_usd,
        platform,
        blockchain,
        A._log_id,
        A._inserted_timestamp
    FROM
        withdraws A
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON A.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.token_address = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
