import os
from utils import load_config
from python.bac_transformer import TransformationsBac


def main():
    # Get the directory of the configs file 
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(os.path.dirname(script_dir))
    configs_dir = os.path.join(base_dir, 'configs', 'configs.json')

    # Configurations
    configs = load_config(configs_dir)
    account_configs = configs['account_configs']
    in_folder_path = os.path.join(base_dir, 'data/in')
    out_folder_path = os.path.join(base_dir, 'data/out')

    # Create a bac transformations object
    testing_file_path = os.path.join(in_folder_path, 'BAC USD 021.csv')
    bac_transformations = TransformationsBac(account_configs, in_folder_path)
    test = bac_transformations.clean_savings_account_statement(csv_path = testing_file_path)



if __name__ == "__main__":
    main()