{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform','version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH across AS (

    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__across_fundsdeposited') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
axelar AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        NULL AS destination_chain_id,
        destination_chain,
        token_address,
        token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__axelar_contractcallwithtoken') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
celer_cbridge AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__celer_cbridge_send') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
hop AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__hop_transfersent') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
meson AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__meson_transfers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
multichain AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__multichain_v7_loganyswapout') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
stargate AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__stargate_swap') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
symbiosis AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__symbiosis_synthesizerequest') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
synapse_tb AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1-tb' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_token_bridge') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
synapse_tbs AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1-tbs' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        NULL AS destination_chain,
        token_address,
        NULL AS token_symbol,
        amount AS amount_unadj,
        _log_id AS _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__synapse_tokenbridgeandswap') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
wormhole AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        'v1' AS version,
        sender,
        receiver,
        destination_chain_receiver,
        destination_chain_id :: STRING AS destination_chain_id,
        destination_chain,
        token_address,
        NULL AS token_symbol,
        amount_unadj,
        _id,
        _inserted_timestamp
    FROM
        {{ ref('silver_bridge__wormhole_transfers') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
all_protocols AS (
    SELECT
        *
    FROM
        across
    UNION ALL
    SELECT
        *
    FROM
        axelar
    UNION ALL
    SELECT
        *
    FROM
        celer_cbridge
    UNION ALL
    SELECT
        *
    FROM
        hop
    UNION ALL
    SELECT
        *
    FROM
        meson
    UNION ALL
    SELECT
        *
    FROM
        multichain
    UNION ALL
    SELECT
        *
    FROM
        stargate
    UNION ALL
    SELECT
        *
    FROM
        symbiosis
    UNION ALL
    SELECT
        *
    FROM
        synapse_tb
    UNION ALL
    SELECT
        *
    FROM
        synapse_tbs
    UNION ALL
    SELECT
        *
    FROM
        wormhole
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        tx_hash,
        event_index,
        bridge_address,
        event_name,
        platform,
        version,
        sender,
        receiver,
        destination_chain_receiver,
        CASE
            WHEN platform IN (
                'stargate',
                'wormhole',
                'meson'
            ) THEN destination_chain_id :: STRING
            WHEN d.chain_id IS NULL THEN destination_chain_id :: STRING
            ELSE d.chain_id :: STRING
        END AS destination_chain_id,
        CASE
            WHEN platform IN (
                'stargate',
                'wormhole',
                'meson'
            ) THEN LOWER(destination_chain)
            WHEN d.chain IS NULL THEN LOWER(destination_chain)
            ELSE LOWER(
                d.chain
            )
        END AS destination_chain,
        b.token_address,
        CASE
            WHEN platform = 'axelar' THEN COALESCE(
                C.token_symbol,
                b.token_symbol
            )
            ELSE C.token_symbol
        END AS token_symbol,
        C.token_decimals AS token_decimals,
        amount_unadj,
        CASE
            WHEN C.token_decimals IS NOT NULL THEN (amount_unadj / pow(10, C.token_decimals))
            ELSE amount_unadj
        END AS amount,
        CASE
            WHEN C.token_decimals IS NOT NULL THEN ROUND(
                amount * p.price,
                2
            )
            ELSE NULL
        END AS amount_usd_unadj,
        _id,
        b._inserted_timestamp
    FROM
        all_protocols b
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON b.token_address = C.contract_address
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON b.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ source(
            'external_gold_defillama',
            'dim_chains'
        ) }}
        d
        ON d.chain_id :: STRING = b.destination_chain_id :: STRING
        OR LOWER(
            d.chain
        ) = LOWER(
            b.destination_chain
        )
)
SELECT
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_hash,
    event_index,
    bridge_address,
    event_name,
    platform,
    version,
    sender,
    receiver,
    destination_chain_receiver,
    destination_chain_id,
    destination_chain,
    token_address,
    token_symbol,
    token_decimals,
    amount_unadj,
    amount,
    CASE
        WHEN amount_usd_unadj < 1e+15 THEN amount_usd_unadj
        ELSE NULL
    END AS amount_usd,
    _id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
    ['_id']
    ) }} AS complete_bridge_activity_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify (ROW_NUMBER() over (PARTITION BY _id
ORDER BY
    _inserted_timestamp DESC)) = 1
