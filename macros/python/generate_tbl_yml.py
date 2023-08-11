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

def generate_yml(model_path, output_dir=None, specific_files=[]):
    """
    Generates a DBT .yml test file for each .sql file in the given directory or for a specified SQL file.
    
    Parameters:
    - model_path (str): The path to the directory containing .sql files or the path to a specific .sql file.
                       If a directory is provided, the function will generate .yml files for all .sql files in that directory.
                       If a specific .sql file path is provided, the function will only generate a .yml file for that file.
    
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
                        columns = [re.split("::| AS ", col)[0].strip() for col in columns if not col.startswith("{")]

                yml_content = "version: 2\nmodels:\n  - name: {}\n    tests:\n      - dbt_utils.unique_combination_of_columns:\n          combination_of_columns:\n            - _LOG_ID\n    columns:\n".format(os.path.basename(sql_filepath).replace('.sql', ''))
                for column in columns:
                    column_type = 'STRING' if column.endswith('_id') or column.endswith('_hash') else 'INTEGER'
                    yml_content += "      - name: {}\n        tests:\n          - not_null\n".format(column)
                    if column_type == 'STRING':
                        yml_content += "          - dbt_expectations.expect_column_values_to_match_regex:\n              regex: 0[xX][0-9a-fA-F]+\n"
                    elif column_type == 'INTEGER':
                        yml_content += "          - dbt_expectations.expect_column_values_to_be_in_type_list:\n              column_type_list:\n                - decimal\n                - float\n                - number\n"

                yml_filename = sql_file.replace('.sql', '.yml')

                if file_exists_in_repo(yml_filename):
                        print(f"Skipped {yml_filename}, already exists...")
                        continue
                
                if output_dir:
                    output_filepath = os.path.join(output_dir, sql_file.replace('.sql', '.yml'))
                else:
                    output_filepath = sql_filepath.replace('.sql', '.yml')

                with open(output_filepath, 'w') as yml_file:
                    yml_file.write(yml_content)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Generate YML files.')
        parser.add_argument('--model_path', required=True, help='Path to the input SQL files.')
        parser.add_argument('--output_dir', default=None, help='Directory to output YML files.')
        args = parser.parse_args()

        generate_yml(args.model_path, args.output_dir)
    except Exception as e:
        print(f"An error occurred in __main__ execution: {e}")
        print(traceback.format_exc())
        raise