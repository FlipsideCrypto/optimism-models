{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name
FROM
    {{ source(
        'crosschain',
        'address_labels'
    ) }}
WHERE
    blockchain = 'optimism'
    AND address LIKE '0x%'
