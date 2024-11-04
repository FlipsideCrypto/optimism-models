{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE', '_inserted_timestamp::DATE'],
    tags = ['stale']
) }}

WITH raw_decoded_logs AS (

    SELECT
        tx_hash,
        event_name,
        contract_address,
        event_index,
        decoded_log AS decoded_flat,
        COALESCE(
            decoded_log :dst,
            decoded_log :_to,
            decoded_log :to
        ) :: STRING AS to_address,
        COALESCE(
            decoded_log :src,
            decoded_log :_from,
            decoded_log :from
        ) :: STRING AS from_address,
        COALESCE(
            decoded_log :wad,
            decoded_log :_value,
            decoded_log :value
        ) :: INT AS token_amount,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        TO_TIMESTAMP_NTZ(modified_timestamp) AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp >= '2021-12-01'
        AND tx_succeeded
        AND contract_address IN (
            '0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c',
            -- v1
            '0x829b1c7b9d024a3915215b8abf5244a4bfc28db4',
            -- v2
            '0x20975da6eb930d592b9d78f451a9156db5e4c77b',
            -- v3
            '0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6',
            -- v4
            '0x3f9da045b0f77d707ea4061110339c4ea8ecfa70',
            -- v5
            '0x4200000000000000000000000000000000000006',
            -- weth for transfers
            '0x4200000000000000000000000000000000000042' -- OP for transfers
        )
        AND event_name IN (
            'BuyOrderFilled',
            'SellOrderFilled',
            'DutchAuctionFilled',
            'Transfer'
        )

{% if is_incremental() %}
AND TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
quix_event_details AS (
    SELECT
        tx_hash,
        event_name,
        event_index,
        decoded_flat :buyer :: STRING AS buyer_address,
        decoded_flat :seller :: STRING AS seller_address,
        decoded_flat :price :: INT AS total_price_raw,
        COALESCE(
            decoded_flat :contractAddress,
            decoded_flat :erc721address
        ) :: STRING AS nft_address,
        decoded_flat :tokenId :: STRING AS tokenId,
        block_number,
        block_timestamp,
        contract_address AS platform_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        raw_decoded_logs
    WHERE
        contract_address IN (
            '0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c',
            -- v1
            '0x829b1c7b9d024a3915215b8abf5244a4bfc28db4',
            -- v2
            '0x20975da6eb930d592b9d78f451a9156db5e4c77b',
            -- v3
            '0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6',
            -- v4
            '0x3f9da045b0f77d707ea4061110339c4ea8ecfa70' -- v5
        )
        AND event_name IN (
            'BuyOrderFilled',
            'SellOrderFilled',
            'DutchAuctionFilled'
        )
),
buyer_filter AS (
    SELECT
        tx_hash,
        buyer_address AS from_address
    FROM
        quix_event_details
),
token_payments_raw AS (
    SELECT
        tx_hash,
        event_index,
        contract_address AS payment_currency,
        to_address,
        from_address,
        token_amount,
        CASE
            WHEN to_address = '0xec1557a67d4980c948cd473075293204f4d280fd' THEN token_amount
            ELSE 0
        END AS platform_fee_raw_,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    token_amount DESC
            ) = 1
            AND platform_fee_raw_ = 0 THEN token_amount
            ELSE 0
        END AS sale_amount_raw_,
        CASE
            WHEN sale_amount_raw_ = 0
            AND platform_fee_raw_ = 0
            AND to_address != '0xec1557a67d4980c948cd473075293204f4d280fd' THEN token_amount
            ELSE 0
        END AS creator_fee_raw_
    FROM
        raw_decoded_logs
        INNER JOIN buyer_filter USING (
            tx_hash,
            from_address
        )
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                quix_event_details
        )
        AND event_name = 'Transfer'
        AND token_amount IS NOT NULL
        AND contract_address IN (
            '0x4200000000000000000000000000000000000006',
            '0x4200000000000000000000000000000000000042'
        )
),
token_payments_agg AS (
    SELECT
        tx_hash,
        payment_currency,
        SUM(platform_fee_raw_) AS platform_fee_raw,
        SUM(sale_amount_raw_) AS sale_amount_raw,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        token_payments_raw
    GROUP BY
        ALL
),
eth_payment_raw AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        value,
        CASE
            WHEN to_address = '0xec1557a67d4980c948cd473075293204f4d280fd' THEN value
            ELSE 0
        END AS platform_fee_raw_,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    value DESC
            ) = 1
            AND platform_fee_raw_ = 0 THEN value
            ELSE 0
        END AS sale_amount_raw_,
        CASE
            WHEN sale_amount_raw_ = 0
            AND platform_fee_raw_ = 0
            AND to_address != '0xec1557a67d4980c948cd473075293204f4d280fd' THEN value
            ELSE 0
        END AS creator_fee_raw_
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        block_timestamp >= '2021-12-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                quix_event_details
        )
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                token_payments_agg
        )
        AND tx_succeeded
        AND identifier != 'CALL_ORIGIN'
        AND value > 0
        AND from_address IN (
            '0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c',
            -- v1
            '0x829b1c7b9d024a3915215b8abf5244a4bfc28db4',
            -- v2
            '0x20975da6eb930d592b9d78f451a9156db5e4c77b',
            -- v3
            '0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6',
            -- v4
            '0x3f9da045b0f77d707ea4061110339c4ea8ecfa70' -- v5
        )

{% if is_incremental() %}
AND TO_TIMESTAMP_NTZ(modified_timestamp) >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
eth_payment_agg AS (
    SELECT
        tx_hash,
        'ETH' AS payment_currency,
        SUM(platform_fee_raw_) AS platform_fee_raw,
        SUM(sale_amount_raw_) AS sale_amount_raw,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        eth_payment_raw
    GROUP BY
        ALL
),
all_payment AS (
    SELECT
        *
    FROM
        token_payments_agg
    UNION ALL
    SELECT
        *
    FROM
        eth_payment_agg
),
quix_event_base AS (
    SELECT
        tx_hash,
        event_name,
        IFF(
            event_name = 'BuyOrderFilled',
            'bid_won',
            'sale'
        ) AS event_type,
        event_index,
        buyer_address,
        seller_address,
        nft_address,
        tokenId,
        payment_currency,
        total_price_raw,
        platform_fee_raw,
        sale_amount_raw,
        creator_fee_raw,
        platform_fee_raw + creator_fee_raw AS total_fees_raw,
        block_number,
        block_timestamp,
        platform_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        _log_id,
        _inserted_timestamp
    FROM
        quix_event_details
        INNER JOIN all_payment USING (tx_hash)
),
tx_data AS (
    SELECT
        tx_hash,
        tx_fee,
        input_data
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= '2021-12-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                quix_event_base
        )

{% if is_incremental() %}
AND TO_TIMESTAMP_NTZ(modified_timestamp) >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
),
nft_transfers AS (
    SELECT
        tx_hash,
        to_address AS buyer_address,
        contract_address AS nft_address,
        tokenId,
        erc1155_value
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-12-01'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                quix_event_base
        )

{% if is_incremental() %}
AND TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
    SELECT
        MAX(
            _inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY tx_hash,
    nft_address,
    tokenid
    ORDER BY
        event_index DESC
) = 1
)
SELECT
    tx_hash,
    event_name,
    event_type,
    event_index,
    buyer_address,
    seller_address,
    nft_address,
    tokenId,
    erc1155_value,
    payment_currency AS currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    sale_amount_raw,
    block_number,
    block_timestamp,
    platform_address,
    'quix' AS platform_name,
    CASE
        WHEN platform_address = '0xe5c7b4865d7f2b08faadf3f6d392e6d6fa7b903c' THEN 'quix ExchangeV1'
        WHEN platform_address = '0x829b1c7b9d024a3915215b8abf5244a4bfc28db4' THEN 'quix ExchangeV2'
        WHEN platform_address = '0x20975da6eb930d592b9d78f451a9156db5e4c77b' THEN 'quix ExchangeV3'
        WHEN platform_address = '0x065e8a87b8f11aed6facf9447abe5e8c5d7502b6' THEN 'quix ExchangeV4'
        WHEN platform_address = '0x3f9da045b0f77d707ea4061110339c4ea8ecfa70' THEN 'quix ExchangeV5'
    END AS platform_exchange_version,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    _log_id,
    _inserted_timestamp
FROM
    quix_event_base
    INNER JOIN tx_data USING (tx_hash)
    INNER JOIN nft_transfers USING (
        tx_hash,
        nft_address,
        tokenId
    ) qualify ROW_NUMBER() over (
        PARTITION BY nft_log_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
