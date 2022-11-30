{% macro task_get_token_reads() %}
    {% set sql %}
    EXECUTE IMMEDIATE 
    'create or replace task bronze_api.get_token_reads
    warehouse = DBT_CLOUD_OPTIMISM
    allow_overlapping_execution = false 
    schedule = \'300 minute\' 
    as 
    BEGIN 
    INSERT INTO
    bronze_api.token_reads(
        contract_address,
        block_number,
        function_sig,
        function_input,
        read_result,
        _inserted_timestamp
) 
with base as (
select 
contract_address,
created_block
from silver.relevant_token_contracts
where contract_address not in (select contract_address from bronze_api.token_reads)
limit 500
)
, function_sigs as (
select \'0x313ce567\' as function_sig, \'decimals\' as function_name
union select \'0x06fdde03\', \'name\'
union select \'0x95d89b41\', \'symbol\'
),
all_reads as (
select *
from base
join function_sigs on 1=1
),
ready_reads as (
select 
contract_address,
created_block,
function_sig,
concat(\'[!\',contract_address,\'!,\', created_block, \',!\',function_sig,\'!,!!]\') as read_input1,
replace(read_input1, $$!$$,$$\'$$) as read_input
from all_reads
)
, batch_reads as (
select concat(\'[\',listagg(read_input,\',\'),\']\') as batch_read
from ready_reads
),
results as (
select
    ethereum.streamline.udf_json_rpc_read_calls(
        node_url,
        headers,
        parse_json(batch_read)
    ) as read_output
from batch_reads
join streamline.crosschain.node_mapping 
on 1=1 and chain = \'optimism\'
where exists (select 1 from ready_reads limit 1)
)
, final as (
select 
value:id::string as read_id,
value:result::string as read_result,
split(read_id,\'-\') as read_id_object,
read_id_object[0]::string as contract_address,
read_id_object[1]::string as block_number,
read_id_object[2]::string as function_sig,
read_id_object[3]::string as function_input
from results,
lateral flatten(input=> read_output[0]:data)
)
select 
contract_address,
block_number,
function_sig,
function_input,
read_result,
sysdate()::timestamp as _inserted_timestamp
from final;
end;'

{% endset %}
    {% do run_query(sql) %}

{% if target.database.upper() == 'OPTIMISM' %}
    {% set sql %}
        alter task bronze_api.get_token_reads resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}

{% endmacro %}