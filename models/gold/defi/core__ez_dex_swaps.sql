{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI, UNISWAP, CURVE, SYNTHETIX, VELODROME, WOOFI, FRAX, KYBERSWAP, HASHFLOW, DODO',
                'PURPOSE': 'DEX, SWAPS'
            }
        }
    }
) }}

SELECT
    *
FROM
  {{ ref('defi__ez_dex_swaps') }}