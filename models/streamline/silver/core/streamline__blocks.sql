{{ config (
    materialized = "view",
    tags = ['streamline_core_complete']
) }}


{% if execute %}
{% set height = run_query('SELECT streamline.udf_get_chainhead()') %}
{% set block_height = height.columns[0].values()[0] %}
{% else %}
{% set block_height = 0 %}
{% endif %}

SELECT
    height as block_number
FROM
    TABLE(streamline.udtf_get_base_table({{block_height}}))