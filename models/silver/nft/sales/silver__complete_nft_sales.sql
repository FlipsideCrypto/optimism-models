{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform_exchange_version'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg','heal']
) }}

WITH nft_base_models AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value :: STRING AS erc1155_value,
        tokenId,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        tx_fee,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        input_data,
        nft_log_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__quix_sales') }}

{% if is_incremental() and 'quix' not in var('HEAL_CURATED_MODEL') %}
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
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__quix_seaport_sales') }}

{% if is_incremental() and 'quix_seaport' not in var('HEAL_CURATED_MODEL') %}
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
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__seaport_1_1_sales') }}

{% if is_incremental() and 'seaport_1_1' not in var('HEAL_CURATED_MODEL') %}
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
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__seaport_1_4_sales') }}

{% if is_incremental() and 'seaport_1_4' not in var('HEAL_CURATED_MODEL') %}
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
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__seaport_1_5_sales') }}

{% if is_incremental() and 'seaport_1_5' not in var('HEAL_CURATED_MODEL') %}
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
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    seller_address,
    buyer_address,
    nft_address,
    erc1155_value :: STRING AS erc1155_value,
    tokenId,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    tx_fee,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    input_data,
    nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    {{ ref('silver__seaport_1_6_sales') }}

{% if is_incremental() and 'seaport_1_6' not in var('HEAL_CURATED_MODEL') %}
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
prices_raw AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        price AS hourly_prices
    FROM
        {{ ref('price__ez_hourly_token_prices') }}
    WHERE
        token_address IN (
            SELECT
                DISTINCT currency_address
            FROM
                nft_base_models
        )
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                nft_base_models
        )
),
all_prices AS (
    SELECT
        HOUR,
        symbol,
        token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    UNION ALL
    SELECT
        HOUR,
        'ETH' AS symbol,
        'ETH' AS token_address,
        decimals,
        hourly_prices
    FROM
        prices_raw
    WHERE
        token_address = '0x4200000000000000000000000000000000000006'
),
eth_price AS (
    SELECT
        HOUR,
        hourly_prices AS eth_price_hourly
    FROM
        prices_raw
    WHERE
        token_address = '0x4200000000000000000000000000000000000006'
),
contracts_decimal AS (
    SELECT
        contract_address AS address_contracts,
        token_symbol AS symbol_contracts,
        token_decimals AS decimals_contracts
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        contract_address IN (
            SELECT
                currency_address
            FROM
                nft_base_models
        )
),
final_base AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        platform_name,
        platform_exchange_version,
        NULL AS aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        erc1155_value,
        tokenId,
        COALESCE(
            p.symbol,
            symbol_contracts
        ) AS currency_symbol,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0x4200000000000000000000000000000000000042',
                '0x4200000000000000000000000000000000000006'
            ) THEN total_price_raw / pow(
                10,
                18
            )
            ELSE COALESCE (
                total_price_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                total_price_raw
            )
        END AS price,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            price * COALESCE(
                hourly_prices,
                0
            )
        ) AS price_usd,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0x4200000000000000000000000000000000000042',
                '0x4200000000000000000000000000000000000006'
            ) THEN total_fees_raw / pow(
                10,
                18
            )
            ELSE COALESCE (
                total_fees_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                total_fees_raw
            )
        END AS total_fees,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            total_fees * COALESCE(
                hourly_prices,
                0
            )
        ) AS total_fees_usd,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0x4200000000000000000000000000000000000042',
                '0x4200000000000000000000000000000000000006'
            ) THEN platform_fee_raw / pow(
                10,
                18
            )
            ELSE COALESCE (
                platform_fee_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                platform_fee_raw
            )
        END AS platform_fee,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            platform_fee * COALESCE(
                hourly_prices,
                0
            )
        ) AS platform_fee_usd,
        CASE
            WHEN currency_address IN (
                'ETH',
                '0x4200000000000000000000000000000000000042',
                '0x4200000000000000000000000000000000000006'
            ) THEN creator_fee_raw / pow(
                10,
                18
            )
            ELSE COALESCE (
                creator_fee_raw / pow(10, COALESCE(p.decimals, decimals_contracts)),
                creator_fee_raw
            )
        END AS creator_fee,
        IFF(
            COALESCE(
                p.decimals,
                decimals_contracts
            ) IS NULL,
            0,
            creator_fee * COALESCE(
                hourly_prices,
                0
            )
        ) AS creator_fee_usd,
        tx_fee,
        tx_fee * eth_price_hourly AS tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        nft_log_id,
        input_data,
        _log_id,
        b._inserted_timestamp
    FROM
        nft_base_models b
        LEFT JOIN all_prices p
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = p.hour
        AND b.currency_address = p.token_address
        LEFT JOIN eth_price e
        ON DATE_TRUNC(
            'hour',
            b.block_timestamp
        ) = e.hour
        LEFT JOIN contracts_decimal C
        ON b.currency_address = C.address_contracts
),

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
heal_model AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        event_type,
        platform_address,
        COALESCE(
            a2.aggregator,
            platform_name
        ) AS platform_name,
        platform_exchange_version,
        COALESCE(
            aggregator_name,
            A.aggregator
        ) AS aggregator_name,
        seller_address,
        buyer_address,
        nft_address,
        C.token_name AS project_name,
        erc1155_value,
        tokenId,
        currency_symbol,
        currency_address,
        total_price_raw,
        total_fees_raw,
        platform_fee_raw,
        creator_fee_raw,
        price,
        price_usd,
        total_fees,
        total_fees_usd,
        platform_fee,
        platform_fee_usd,
        creator_fee,
        creator_fee_usd,
        tx_fee,
        tx_fee_usd,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        nft_log_id,
        input_data,
        _log_id,
        t._inserted_timestamp
    FROM
        {{ this }}
        t
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON t.nft_address = C.contract_address
        LEFT JOIN {{ ref('silver__aggregator_list') }} A
        ON RIGHT(
            t.input_data,
            8
        ) = A.aggregator_identifier
        AND aggregator_type = 'calldata'
        LEFT JOIN {{ ref('silver__aggregator_list') }}
        a2
        ON t.origin_to_address = a2.aggregator_identifier
        AND a2.aggregator_type = 'router'
    WHERE
        (
            t.block_number IN (
                SELECT
                    DISTINCT t1.block_number AS block_number
                FROM
                    {{ this }}
                    t1
                WHERE
                    t1.project_name IS NULL
                    AND _inserted_timestamp < (
                        SELECT
                            MAX(
                                _inserted_timestamp
                            ) - INTERVAL '36 hours'
                        FROM
                            {{ this }}
                    )
                    AND EXISTS (
                        SELECT
                            1
                        FROM
                            {{ ref('silver__contracts') }} C
                        WHERE
                            C._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
                            AND C.token_name IS NOT NULL
                            AND C.contract_address = t1.nft_address)
                    )
            )
            OR (
                t.block_number IN (
                    SELECT
                        DISTINCT t1.block_number AS block_number
                    FROM
                        {{ this }}
                        t1
                    WHERE
                        t1.aggregator_name IS NULL
                        AND _inserted_timestamp < (
                            SELECT
                                MAX(
                                    _inserted_timestamp
                                ) - INTERVAL '36 hours'
                            FROM
                                {{ this }}
                        )
                        AND EXISTS (
                            SELECT
                                1
                            FROM
                                {{ ref('silver__aggregator_list') }} A
                            WHERE
                                A._inserted_timestamp > DATEADD('DAY', -2, SYSDATE())
                                AND A.aggregator_type = 'calldata'
                                AND RIGHT(
                                    t1.input_data,
                                    8
                                ) = A.aggregator_identifier
                        )
                )
            )
            OR (
                t.block_number IN (
                    SELECT
                        DISTINCT t1.block_number AS block_number
                    FROM
                        {{ this }}
                        t1
                    WHERE
                        t1.origin_to_address IN (
                            SELECT
                                aggregator_identifier
                            FROM
                                {{ ref('silver__aggregator_list') }}
                            WHERE
                                aggregator_type = 'router'
                                AND _inserted_timestamp >= DATEADD('DAY', -2, SYSDATE()))
                                AND _inserted_timestamp < (
                                    SELECT
                                        MAX(
                                            _inserted_timestamp
                                        ) - INTERVAL '36 hours'
                                    FROM
                                        {{ this }}
                                )
                                AND EXISTS (
                                    SELECT
                                        1
                                    FROM
                                        {{ ref('silver__aggregator_list') }}
                                        a2
                                    WHERE
                                        a2._inserted_timestamp > DATEADD('DAY', -2, SYSDATE())
                                        AND t1.origin_to_address = a2.aggregator_identifier
                                        AND a2.aggregator_type = 'router')
                                )
                        )
                ),
                {% endif %}

                combined AS (
                    SELECT
                        block_number,
                        block_timestamp,
                        tx_hash,
                        event_index,
                        event_type,
                        platform_address,
                        COALESCE(
                            a2.aggregator,
                            platform_name
                        ) AS platform_name,
                        platform_exchange_version,
                        COALESCE(
                            aggregator_name,
                            A.aggregator
                        ) AS aggregator_name,
                        seller_address,
                        buyer_address,
                        nft_address,
                        C.token_name AS project_name,
                        erc1155_value,
                        tokenId,
                        currency_symbol,
                        currency_address,
                        total_price_raw,
                        total_fees_raw,
                        platform_fee_raw,
                        creator_fee_raw,
                        price,
                        price_usd,
                        total_fees,
                        total_fees_usd,
                        platform_fee,
                        platform_fee_usd,
                        creator_fee,
                        creator_fee_usd,
                        tx_fee,
                        tx_fee_usd,
                        origin_from_address,
                        origin_to_address,
                        origin_function_signature,
                        nft_log_id,
                        input_data,
                        _log_id,
                        b._inserted_timestamp,
                        {{ dbt_utils.generate_surrogate_key(
                            ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
                        ) }} AS complete_nft_sales_id,
                        SYSDATE() AS inserted_timestamp,
                        SYSDATE() AS modified_timestamp,
                        '{{ invocation_id }}' AS _invocation_id
                    FROM
                        final_base b
                        LEFT JOIN {{ ref('silver__contracts') }} C
                        ON b.nft_address = C.contract_address
                        LEFT JOIN {{ ref('silver__aggregator_list') }} A
                        ON RIGHT(
                            b.input_data,
                            8
                        ) = A.aggregator_identifier
                        AND A.aggregator_type = 'calldata'
                        LEFT JOIN {{ ref('silver__aggregator_list') }}
                        a2
                        ON b.origin_to_address = a2.aggregator_identifier
                        AND a2.aggregator_type = 'router'

{% if is_incremental() and var(
    'HEAL_MODEL'
) %}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    currency_symbol,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    price,
    price_usd,
    total_fees,
    total_fees_usd,
    platform_fee,
    platform_fee_usd,
    creator_fee,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    nft_log_id,
    input_data,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index', 'nft_address','tokenId','platform_exchange_version']
    ) }} AS complete_nft_sales_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    heal_model
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_type,
    platform_address,
    platform_name,
    platform_exchange_version,
    aggregator_name,
    seller_address,
    buyer_address,
    nft_address,
    project_name,
    erc1155_value,
    tokenId,
    currency_symbol,
    currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    price,
    price_usd,
    total_fees,
    total_fees_usd,
    platform_fee,
    platform_fee_usd,
    creator_fee,
    creator_fee_usd,
    tx_fee,
    tx_fee_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    nft_log_id,
    input_data,
    _log_id,
    _inserted_timestamp,
    complete_nft_sales_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    combined qualify (ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
