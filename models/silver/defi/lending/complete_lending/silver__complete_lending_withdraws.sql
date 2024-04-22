{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH aave as (

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
        'optimism' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_withdraws') }}

{% if is_incremental() and 'aave' not in var('HEAL_CURATED_MODEL') %}
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

granary as (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        granary_token AS protocol_market,
        granary_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        depositor_address,
        platform,
        'optimism' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__granary_withdraws') }}

    {% if is_incremental() and 'granary' not in var('HEAL_CURATED_MODEL') %}
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

exactly as (
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
        'optimism' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__exactly_withdraws') }}

    {% if is_incremental() and 'exactly' not in var('HEAL_CURATED_MODEL') %}
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

sonne as (
        
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
        'optimism' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__sonne_withdraws') }}

    {% if is_incremental() and 'sonne' not in var('HEAL_CURATED_MODEL') %}
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

tarot as (

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
        'optimism' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__tarot_withdraws') }}

{% if is_incremental() and 'tarot' not in var('HEAL_CURATED_MODEL') %}
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

withdraws_union as (
    SELECT
        *
    FROM
        aave
    UNION ALL
    SELECT
        *
    FROM
        granary
    UNION ALL
    SELECT
        *
    FROM
        exactly
    UNION ALL
    SELECT
        *
    FROM
        sonne
    UNION ALL
    SELECT
        *
    FROM
        tarot
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
            WHEN platform IN ('Tarot','Sonne') THEN 'Redeem'
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
        A.blockchain,
        A._log_id,
        A._inserted_timestamp
    FROM
        withdraws_union A
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
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
