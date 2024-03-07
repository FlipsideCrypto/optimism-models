{{ config(
    materialized = 'incremental',
    unique_key = 'aggregator_identifier',
    merge_update_columns = ['aggregator_identifier', 'aggregator', 'aggregator_type']
) }}


WITH calldata_aggregators AS (
    SELECT
        *
    FROM
        (
            VALUES
                ('0', '0', 'calldata', '2020-01-01')
        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

platform_routers as (
SELECT
        *
    FROM
        (
            VALUES
                ('0xc9605a76b0370e148b4a510757685949f13248c7', 'element', 'router', '2024-03-07'),
                ('0xbbbbbbbe843515689f3182b748b5671665541e58', 'bluesweep', 'router', '2024-03-07')

        ) t (aggregator_identifier, aggregator, aggregator_type, _inserted_timestamp)
),

combined as (
SELECT * 
FROM
    calldata_aggregators

UNION ALL 

SELECT *
FROM
    platform_routers
)

SELECT 
    aggregator_identifier,
    aggregator, 
    aggregator_type,
    _inserted_timestamp
FROM combined

qualify row_number() over (partition by aggregator_identifier order by _inserted_timestamp desc ) = 1 

