{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','nft','curated']
) }}

WITH settings AS (

    SELECT
        '2023-11-20' AS start_date,
        '0x2317d8b224328644759319dffa2a5da77c72e0e9' AS main_address,
        '0x99302eaef620fba2131bfeff336188961d62747a' AS fee_address,
        '0x4200000000000000000000000000000000000006' AS wrapped_native_address
),
raw AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        decoded_log AS decoded_flat,
        IFF(
            event_name LIKE '%Buy%',
            'bid_won',
            'sale'
        ) AS event_type,
        decoded_log :erc20Token :: STRING AS currency_address_raw,
        COALESCE(
            decoded_log :erc20TokenAmount,
            decoded_log :erc20FillAmount
        ) :: INT AS amount_raw,
        COALESCE(
            decoded_log :erc721Token,
            decoded_log :erc1155Token
        ) :: STRING AS nft_address,
        COALESCE(
            decoded_log :erc721TokenId,
            decoded_log :erc1155TokenId
        ) :: STRING AS tokenid,
        decoded_log :erc1155FillAmount :: STRING AS erc1155_value,
        IFF(
            erc1155_value IS NULL,
            'erc721',
            'erc1155'
        ) AS nft_type,
        decoded_log :maker :: STRING AS maker,
        decoded_log :taker :: STRING AS taker,
        IFF(
            event_name LIKE '%Buy%',
            taker,
            maker
        ) AS seller_address,
        IFF(
            event_name LIKE '%Buy%',
            maker,
            taker
        ) AS buyer_address,
        decoded_log :fees AS fees_array,
        decoded_log :orderHash :: STRING AS orderhash,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS intra_grouping_seller_fill,
        block_timestamp,
        block_number,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                start_date
            FROM
                settings
        )
        AND contract_address = (
            SELECT
                main_address
            FROM
                settings
        )
        AND event_name IN (
            'ERC721BuyOrderFilled',
            'ERC721SellOrderFilled',
            'ERC1155SellOrderFilled',
            'ERC1155BuyOrderFilled'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
raw_fees AS (
    SELECT
        tx_hash,
        event_index,
        INDEX,
        VALUE :amount :: INT AS fee_amount_raw,
        VALUE :recipient :: STRING AS fee_recipient,
        CASE
            WHEN fee_recipient = (
                SELECT
                    fee_address
                FROM
                    settings
            ) THEN fee_amount_raw
            ELSE 0
        END AS platform_amount_raw,
        CASE
            WHEN fee_recipient != (
                SELECT
                    fee_address
                FROM
                    settings
            ) THEN fee_amount_raw
            ELSE 0
        END AS creator_amount_raw
    FROM
        raw,
        LATERAL FLATTEN (
            input => fees_array
        )
),
raw_fees_agg AS (
    SELECT
        tx_hash,
        event_index,
        SUM(platform_amount_raw) AS platform_fee_raw_,
        SUM(creator_amount_raw) AS creator_fee_raw_
    FROM
        raw_fees
    GROUP BY
        ALL
),
new_base AS (
    SELECT
        tx_hash,
        intra_grouping_seller_fill,
        event_index,
        event_name,
        decoded_flat,
        event_type,
        currency_address_raw,
        amount_raw,
        nft_address,
        tokenid,
        erc1155_value,
        nft_type,
        maker,
        taker,
        seller_address,
        buyer_address,
        amount_raw AS total_price_raw,
        COALESCE(
            platform_fee_raw_,
            0
        ) + COALESCE(
            creator_fee_raw_,
            0
        ) AS total_fees_raw,
        COALESCE(
            platform_fee_raw_,
            0
        ) AS platform_fee_raw,
        COALESCE(
            creator_fee_raw_,
            0
        ) AS creator_fee_raw,
        fees_array,
        orderhash,
        block_timestamp,
        block_number,
        _log_id,
        _inserted_timestamp
    FROM
        raw
        LEFT JOIN raw_fees_agg USING (
            tx_hash,
            event_index
        )
),
tx_data AS (
    SELECT
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature,
        tx_fee,
        input_data
    FROM
        {{ ref('core__fact_transactions') }}
    WHERE
        block_timestamp :: DATE >= (
            SELECT
                start_date
            FROM
                settings
        )
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                new_base
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    event_name,
    decoded_flat,
    event_type,
    (
        SELECT
            main_address
        FROM
            settings
    ) AS platform_address,
    'element' AS platform_name,
    'element v1' AS platform_exchange_version,
    intra_grouping_seller_fill,
    currency_address_raw,
    amount_raw,
    nft_address,
    tokenid,
    erc1155_value,
    nft_type,
    maker,
    taker,
    seller_address,
    buyer_address,
    IFF(
        currency_address_raw = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee',
        'ETH',
        currency_address_raw
    ) AS currency_address,
    total_price_raw,
    total_fees_raw,
    platform_fee_raw,
    creator_fee_raw,
    fees_array,
    orderhash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    tx_fee,
    input_data,
    _log_id,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    _inserted_timestamp
FROM
    new_base
    INNER JOIN tx_data USING (tx_hash) qualify ROW_NUMBER() over (
        PARTITION BY nft_log_id
        ORDER BY
            _inserted_timestamp DESC
    ) = 1
