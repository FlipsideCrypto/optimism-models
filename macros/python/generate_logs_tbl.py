import snowflake.connector
import yaml
import argparse
import os
import json
import subprocess
import traceback

def get_dbt_profile():
    """
    Get the directory where dbt looks for profiles.yml using dbt debug and
    the profile name from dbt_project.yml.
    """
    try:
        output = subprocess.check_output(['dbt', 'debug']).decode('utf-8')
        profiles_dir = None
        for line in output.splitlines():
            if 'Using profiles.yml file at' in line:
                profiles_dir = os.path.dirname(line.replace('Using profiles.yml file at', '').strip())
                break
        if not profiles_dir:
            raise ValueError("DBT_PROFILES_DIR not found in dbt debug output")

        with open("dbt_project.yml", 'r') as f:
            dbt_config = yaml.safe_load(f)
            profile_name = dbt_config.get('profile')
            if not profile_name:
                raise ValueError("Profile not found in dbt_project.yml")

        return profiles_dir, profile_name

    except Exception as e:
        print(f"Error in get_dbt_profile function: {e}")
        print(traceback.format_exc())
        raise

def snowflake_connection(profile_name, profiles_dir, target):
    """
    Define and create connection to Snowflake by accessing local database environment/profile.
    """
    local_profile_path = os.path.join(profiles_dir, 'profiles.yml')
    if not os.path.exists(local_profile_path):
        raise ValueError(f"{local_profile_path} does not exist")
    
    with open(local_profile_path, 'r') as f:
        profiles = yaml.safe_load(f)

    config = profiles[profile_name]['outputs'][target]
    try:
        conn = snowflake.connector.connect(**config)
    except Exception as e:
        print(f"Error establishing Snowflake connection: {e}")
        print(traceback.format_exc())
        raise
    return conn

def file_exists_in_repo(filename):
    """
    Check if a file exists in the given directory or its subdirectories.
    """
    root_dir = os.getcwd()

    for dirpath, dirnames, filenames in os.walk(root_dir):
        if filename in filenames:
            return True
    return False

def generate_addr_clause(contract_addresses):
    if contract_addresses:
        formatted_addresses = ', '.join([f"'{address}'" for address in contract_addresses if address.startswith('0x')])
        if formatted_addresses:
            return f"AND contract_address IN ({formatted_addresses})"
    return ""

def get_key_types(conn, blockchain, schema, name, contract_addresses, topic_0):
    """
    Execute a Snowflake SQL query to fetch the keys and their types.
    """
    if not topic_0 or len(topic_0) < 1:
        print(f"Skipped {schema}__{name} due to missing or incorrect event.")
        return {}

    contract_address_clause = generate_addr_clause(contract_addresses)
    
    key_types_query = f"""
    WITH base_data AS (
        SELECT 
            contract_address,
            topics[0] AS topic_0,
            decoded_flat
        FROM 
            {blockchain}.silver.decoded_logs
        WHERE 
            topics[0] :: STRING = '{topic_0}'
            {contract_address_clause}
        LIMIT 1
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
    cursor.execute(key_types_query)
    row = cursor.fetchone()
    if not row or not row[0]:
        print(f"No key types found for {name}, contract: {contract_addresses}, topic: {topic_0} on {blockchain}")
        return {}
    key_types_str = row[0]
    key_types_dict = json.loads(key_types_str)
    cursor.close()
    
    return key_types_dict

def generate_sql(name, contract_addresses, topic_0, keys_types):
    """
    Generate the desired DBT model based on the contract_address, topic_0, and keys_types. 
    This does not execute a Snowflake SQL query, it simply creates the DBT model.
    """
    contract_address_clause = generate_addr_clause(contract_addresses)
    column_exception_mapping = {
        "from": "from_address",
        "to": "to_address"
    }

    materialized = "incremental"
    unique_key = "_log  _id"
    tags = "['non_realtime']"

    base_evt_query = f"""
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
        '{name}' AS name,
        event_index,
        topics[0] :: STRING AS topic_0,
        event_name,      
        {', '.join([f'decoded_flat:"{key}"::{type} AS {column_exception_mapping.get(key, key)}' for key, type in keys_types.items()])},
        decoded_flat,
        data,
        event_removed,
        tx_status,
        _log_id,
        _inserted_timestamp
    FROM 
        {{{{ ref('silver__decoded_logs') }}}}
    WHERE 
        topics[0] :: STRING = '{topic_0}'
        {contract_address_clause}

    {{% if is_incremental() %}}
    AND _inserted_timestamp >= (
    SELECT MAX(_inserted_timestamp) :: DATE
    FROM {{{{ this }}}}
    )
    {{% endif %}}
    """
    return base_evt_query

def main(config_file, output_dir, target):
    
    print("Generating tables...")
    conn = snowflake_connection(profile_name, profiles_dir, target)

    os.makedirs(output_dir, exist_ok=True)

    with open(config_file, 'r') as file:
        config = json.load(file)

    for item in config:
        try:
            blockchain = item.get('blockchain','').lower()
            schema = item.get('schema','').lower()
            name = item.get('name','').lower()
            contract_addresses = item.get('contract_address', [])
            if not isinstance(contract_addresses, list):
                contract_addresses = [contract_addresses.lower()]
            topic_0 = item.get('topic_0','').lower()
        
            filename = f"{schema}__{name}.sql"
            if file_exists_in_repo(filename):
                print(f"Skipped {schema}__{name}, already exists...")
                continue

            keys_types = get_key_types(conn, blockchain, schema, name, contract_addresses, topic_0)
            if not keys_types:
                continue

            sql_query = generate_sql(name, contract_addresses, topic_0, keys_types)


            with open(f"{output_dir}/{filename}", 'w') as file:
                file.write(sql_query)

            print(f"SQL file for {schema}__{name} on {blockchain} generated.")

        except Exception as e:
            print(f"Error processing item {item} in main function: {e}")
            print(traceback.format_exc())
            raise
        
    conn.close()

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Generate SQL files.')
        parser.add_argument('--config_file', default='macros/python/generate_logs_config.json', help='Path to the config file.')
        parser.add_argument('--output_dir', default='models/temp_models', help='Directory to output SQL files.')
        parser.add_argument('--target', default='dev', help='Target environment (default: dev).')
        args = parser.parse_args()

        profiles_dir, profile_name = get_dbt_profile()

        main(args.config_file, args.output_dir, args.target)
    except Exception as e:
        print(f"An error occurred in __main__ execution: {e}")
        print(traceback.format_exc())
        raise