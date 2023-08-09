import snowflake.connector
import yaml
import json

def load_snowflake_config(profile_name):
    with open('/Users/trevor/.dbt/profiles.yml', 'r') as f:
        profiles = yaml.safe_load(f)

    # Extract Snowflake configurations for the desired profile
    config = profiles[profile_name]['outputs']['dev']
    return config

def snowflake_connection(profile_name):
    config = load_snowflake_config(profile_name)

    conn = snowflake.connector.connect(
        user=config['user'],
        role=config['role'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema'],
        authenticator='externalbrowser'
    )
    return conn

def get_key_types(conn, contract_address, topic_0):
    """
    Execute a Snowflake SQL query to fetch the keys and their types.
    """

    query = f"""
    WITH base_data AS (
        SELECT 
            contract_address,
            topics[0] AS topic_0,
            decoded_flat
        FROM 
            optimism.silver.decoded_logs
        WHERE 
            --contract_address = '{contract_address}'
            --AND 
            topics[0] :: STRING = '{topic_0}'
    )

    SELECT
        OBJECT_AGG(DISTINCT key, data_type::VARIANT) AS key_types
    FROM (
        SELECT
            key,
            CASE
                WHEN VALUE::STRING IN ('true', 'false') THEN 'BOOLEAN'
                WHEN IS_DATE(VALUE) THEN 'DATE'
                WHEN TRY_CAST(VALUE::STRING AS INTEGER) IS NOT NULL THEN 'INTEGER'
                ELSE 'STRING'
            END AS data_type
        FROM base_data,
        LATERAL FLATTEN(input => base_data.decoded_flat)
    )
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    row = cursor.fetchone()

    if not row or not row[0]:
        print(f"No key types found for contract: {contract_address}, topic: {topic_0}")
        return {}
    
    key_types_str = row[0]
    key_types_dict = json.loads(key_types_str)
    cursor.close()
    return key_types_dict

def generate_sql(contract_address, topic_0, keys_types):
    """
    Generate the desired SQL based on the contract_address, topic_0, and keys_types.
    """
    materialized = "incremental"
    unique_key = "_log_id"
    tags = "['non_realtime']"

    base_query = f"""
    {{{{ config(
    materialized = '{materialized}',
    unique_key = '{unique_key}',
    tags = {tags}
    ) }}}}
    
    SELECT 
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        {', '.join([f'decoded_flat:"{key}"::{type} AS "{key.upper()}"' for key, type in keys_types.items()])},
        decoded_flat,
        data,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{{{ ref('silver__decoded_logs') }}}}
    WHERE 
        --contract_address = '{contract_address}'
        --AND 
        topics[0] :: STRING = '{topic_0}'
    {{% if is_incremental() %}}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{{{ this }}}}
    )
    {{% endif %}}
    """
    
    return base_query

def main(config_file, output_dir):
    profile_name = "optimism"
    conn = snowflake_connection(profile_name)

    with open(config_file, 'r') as file:
        config = json.load(file)

    for item in config:
        contract_name = item['contract_name']
        contract_address = item['contract_address']
        topic_0 = item['topic_0']
        
        # Get the dynamic keys and their types
        keys_types = get_key_types(conn, contract_address, topic_0)

        if not keys_types:
            continue

        # Generate the SQL based on the dynamic keys/types
        sql_query = generate_sql(contract_address, topic_0, keys_types)

        # Create a unique filename based on the contract and topic
        filename = f"{contract_name}_{contract_address}_{topic_0}.sql".replace('0x', '')

        # Write the SQL query to the file
        with open(f"{output_dir}/{filename}", 'w') as file:
            file.write(sql_query)

        print(f"SQL file for {contract_address}, {topic_0} generated.")

    conn.close()

if __name__ == "__main__":
    config_file = "macros/python/contract_topics.json"
    output_dir = "models/test"
    main(config_file, output_dir)