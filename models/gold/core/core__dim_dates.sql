{{ config(
    materialized = "table",
    tags = ['non_realtime'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}
{{ dbt_date.get_date_dimension(
    '2017-01-01',
    '2027-12-31'
) }}