{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'VELODROME',
    'PURPOSE': 'DEFI, DEX' }}}
) }}

SELECT
    base.block_number AS block_number,
    base.block_timestamp AS block_timestamp,
    base.tx_hash AS tx_hash,
    base.origin_function_signature AS origin_function_signature,
    base.origin_from_address AS origin_from_address,
    base.origin_to_address AS origin_to_address,
    COALESCE(
        g1.gauge_address,
        g0.gauge_address
    ) AS gauge_address,
    COALESCE(
        g1.external_bribe_address,
        g0.external_bribe_address
    ) AS external_bribe_address,
    COALESCE(
        g1.internal_bribe_address,
        g0.internal_bribe_address
    ) AS internal_bribe_address,
    COALESCE(
        g1.pool_address,
        g0.pool_address
    ) AS pool_address,
    COALESCE(
        g1.pool_name,
        g0.pool_name
    ) AS pool_name,
    from_address,
    token_id,
    vote_amount,
    vote_action,
    COALESCE (
        votes_id,
        {{ dbt_utils.generate_surrogate_key(
            ['base.tx_hash', 'base.event_index']
        ) }}
    ) AS ez_votes_id,
    GREATEST(
        COALESCE(
            base.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            g1.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            g0.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            base.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            g1.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            g0.inserted_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp
FROM
    {{ ref('silver__velodrome_votes') }}
    base
    LEFT JOIN {{ ref('velodrome__ez_gauges') }}
    g1
    ON LOWER(
        base.contract_address
    ) = LOWER(
        g1.external_bribe_address
    )
    LEFT JOIN {{ ref('velodrome__ez_gauges') }}
    g0
    ON LOWER(
        base.contract_address
    ) = LOWER(
        g0.internal_bribe_address
    )
