{% macro task_get_abis() %}
    {% set sql %}
    EXECUTE IMMEDIATE 
    'create or replace task bronze_api.get_block_explorer_abis
    warehouse = DBT_CLOUD_OPTIMISM
    allow_overlapping_execution = false 
    schedule = \'30 minute\' 
    as 
    BEGIN 
INSERT INTO
    bronze_api.contract_abis(
        contract_address,
        abi_data,
        _inserted_timestamp
    ) 
    
WITH api_keys AS (
        SELECT
            api_key
        FROM
            crosschain.silver.apis_keys
        WHERE
            api_name = \'op-etherscan\'
    ),
    base AS (
        SELECT
            contract_address
        FROM
            silver.relevant_abi_contracts
        EXCEPT
        SELECT
            contract_address
        FROM
            bronze_api.contract_abis
        WHERE
            abi_data :data :result :: STRING <> \'Max rate limit reached\'
        LIMIT
            100
    )
SELECT
    contract_address,
    ethereum.streamline.udf_api(
        \'GET\',
        CONCAT(
            \'https://api-optimistic.etherscan.io/api?module=contract&action=getabi&address=\',
            contract_address,
            \'&apikey=\',
            api_key
        ),{},{}
    ) AS abi_data,
    SYSDATE()
FROM
    base
    LEFT JOIN api_keys
    ON 1 = 1
    where exists (select 1 from base limit 1);
END;' 

{% endset %}
    {% do run_query(sql) %}

{% if target.database.upper() == 'OPTIMISM' %}
    {% set sql %}
        alter task bronze_api.get_block_explorer_abis resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}

{% endmacro %}
