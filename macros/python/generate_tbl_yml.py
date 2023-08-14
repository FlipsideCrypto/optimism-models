import os
import re
import argparse
import traceback

def file_exists_in_repo(filename):
    """
    Check if a file exists in the given directory or its subdirectories.
    """
    root_dir = os.getcwd()

    for dirpath, dirnames, filenames in os.walk(root_dir):
        if filename in filenames:
            return True
    return False

def generate_yml(model_paths, output_dir=None, specific_files=[], drop_all=False):
    """
    Generates a DBT .yml test file for each .sql file in the given directory or for a specified SQL file.
    
    Parameters:
    - model_paths (list of str): The paths to the directories containing .sql files or the paths to specific .sql files.
                            If a directory is provided for each path, the function will generate .yml files for all .sql files in those directories.
                            If specific .sql file paths are provided, the function will only generate a .yml file for those files.
    
    - output_dir (str, optional): The directory where the .yml files will be saved. If not provided, the .yml files will
                                  be saved in the same directory as the input .sql files or alongside the specified .sql file.
                                  
    - specific_files (list of str, optional): A list of specific .sql filenames for which .yml files should be generated.
                                              This argument is useful when the function is called from another script and
                                              you only want to generate .yml files for specific .sql files in a directory.
                                              If not provided and a directory is given for model_path, .yml files will be generated
                                              for all .sql files in that directory.
    
    Returns:
    None. This function writes .yml files to the specified or default output directory.
    """
    column_type_mapping = {
        tuple(["number", "index", ":: INTEGER"]): "INTEGER",
        tuple(["hash", "signature", "address", "name", "_id", ":: STRING"]): "STRING",
        tuple(["bool", ":: BOOLEAN"]): "BOOLEAN",
        tuple(["flat", "data", ":: VARIANT"]): "VARIANT",
        tuple(["timestamp", ":: TIMESTAMP", ":: DATE"]): "TIMESTAMP"
    }
    skip_column_mapping = ["event_removed"]
    column_test_mapping = {
        "STRING": "dbt_expectations.expect_column_values_to_match_regex:\n              regex: 0[xX][0-9a-fA-F]+\n",
        "INTEGER": "dbt_expectations.expect_column_values_to_be_in_type_list:\n              column_type_list:\n                - DECIMAL\n                - FLOAT\n                - NUMBER\n",
        "TIMESTAMP": "dbt_expectations.expect_column_values_to_be_in_type_list:\n              column_type_list:\n                - TIMESTAMP_LTZ\n"
    }
    for model_path in model_paths:
        specific_files = []
        if os.path.isfile(model_path) and model_path.endswith('.sql'):
            specific_files = [os.path.basename(model_path)]
            model_path = os.path.dirname(model_path)
        elif not specific_files:
            specific_files = [f for f in os.listdir(model_path) if f.endswith('.sql')]

        for root, _, files in os.walk(model_path):
            for sql_file in files:
                if sql_file.endswith('.sql') and (not specific_files or sql_file in specific_files):
                    sql_filepath = os.path.join(root, sql_file)
                    columns = []
                    with open(sql_filepath, 'r') as f:
                        sql_content = f.read()
                        match = re.search(r"SELECT\s+(.*?)\s+FROM", sql_content, re.DOTALL)
                        if match:
                            select_content = match.group(1)
                            columns = [col.strip() for col in select_content.split(",")]
                            columns = [re.split("::| AS ", col)[-1].strip().upper() for col in columns if not col.startswith("{") and col not in skip_column_mapping]

                    yml_content = "version: 2\nmodels:\n  - name: {}\n    tests:\n      - dbt_utils.unique_combination_of_columns:\n          combination_of_columns:\n            - _LOG_ID\n    columns:\n".format(os.path.basename(sql_filepath).replace('.sql', ''))
                    for column in columns:
                        column_type = 'STRING'
                        column_lower = column.lower()
                        
                        for key_tuple, value in column_type_mapping.items():
                            if any(substring in column_lower for substring in key_tuple):
                                column_type = value
                                break

                        yml_content += "      - name: {}\n        tests:\n          - not_null\n".format(column)
                        if column_type in column_test_mapping:
                            yml_content += "          - " + column_test_mapping[column_type]

                    yml_filename = sql_file.replace('.sql', '.yml')

                    yml_exists = file_exists_in_repo(yml_filename)

                    if yml_exists:
                        if drop_all:
                            print(f"Dropped and replaced {yml_filename}.")
                        else:
                            print(f"Skipped {yml_filename}, already exists...")
                            continue
                    else:
                        print(f"Generated {yml_filename}.")

                    if output_dir:
                        output_filepath = os.path.join(output_dir, sql_file.replace('.sql', '.yml'))
                    else:
                        output_filepath = sql_filepath.replace('.sql', '.yml')

                    with open(output_filepath, 'w') as yml_file:
                        yml_file.write(yml_content)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Generate YML files.')
        parser.add_argument('--model_path', required=True, nargs='+', help='Path(s) to the input SQL files.')
        parser.add_argument('--output_dir', default=None, help='Directory to output YML files.')
        parser.add_argument('--drop_all', action='store_true', help='Drop and replace all existing YML files.')
        args = parser.parse_args()

        generate_yml(model_paths=args.model_path, output_dir=args.output_dir, drop_all=args.drop_all)
    except Exception as e:
        print(f"An error occurred in __main__ execution: {e}")
        print(traceback.format_exc())
        raise