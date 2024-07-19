import pandas as pd
import os
import numpy as np
import hashlib
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Define a function to read a CSV file from a relative or absolute path and return a DataFrame with all columns as strings
def read_csv_file(file_path):
    """
    Reads a CSV file and returns its content as a pandas DataFrame with all columns as strings.

    Args:
    file_path: A string representing the relative or absolute path of the CSV file.
    
    Returns:
    A pandas DataFrame containing the data from the CSV file with all columns as type string.
    """
    try:
        # Read the CSV file into a pandas DataFrame with all columns as strings
        dataframe = pd.read_csv(file_path, dtype=str)
        
        # Return the DataFrame
        return dataframe

    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

def clean_column_values(df):
    # Clean all string columns in a DataFrame
    
    # Define a lambda function that replaces multiple whitespaces with a single space
    # and strips leading and trailing whitespaces
    clean_str = lambda x: ' '.join(str(x).split()) if isinstance(x, str) else x
    
    # Apply this function to every element of the DataFrame (assuming it's a string)
    return df.applymap(clean_str)

def drop_null_or_empty_first_column(data:pd.DataFrame) -> pd.DataFrame:
    """
    Drops rows from the DataFrame where the first column has null or empty values.

    Args:
    data: A pandas DataFrame.
    
    Returns:
    A pandas DataFrame with rows containing null or empty values in the first column removed.
    """
    # Get the name of the first column
    first_column_name = data.columns[0]
    
    # Drop rows where the first column is null or empty
    data_cleaned = data[data[first_column_name].notnull() & (data[first_column_name].str.strip() != '')]
    
    return data_cleaned


# Function to clean the specified columns in the dataframe
def remove_unwanted_chars(df, columns):
    # Check if columns parameter is a single string, 
    # if so, convert it to a list to simplify processing
    if isinstance(columns, str):
        columns = [columns]

    # Regular expression pattern to match anything that's not a digit or period
    pattern = r'[^\d.\-]'
    
    # Loop through each column and clean it
    for column in columns:
        # Use .str.replace() to replace non-numeric/non-period characters with ''
        df[column] = df[column].astype(str).str.replace(pattern, '', regex=True)
        # Convert column back to a numeric type (float)
        df[column] = pd.to_numeric(df[column], errors='coerce')

    return df

def clean_bac_cc_stmt(dataframe: pd.DataFrame):    
    
    # Remove the empty rows.
    data = drop_null_or_empty_first_column(dataframe)
    
    # Remove the commas and letters from the field list
    data = remove_unwanted_chars(data, ["Monto lempiras", "Monto dólares"])

    # Create a new column named "currency" with the following condition
    # if data["Monto Lempiras"] == np.nan then "HNL" ELSE "USD"
    data['currency'] = np.where(data["Monto lempiras"].isna(), "USD", "HNL")

    # Add the exchange rate. Right now it is hardcoded # TODO: USE AN API LATER 
    data['exchange_rate'] = np.where(data["currency"] == "USD", 24.8, 1.0)

    # Coalesce the two columns "Monto lempiras" and "Monto dólares"
    data['amount'] = data['Monto lempiras'].fillna(data['Monto dólares'])

    # Drop the columns "Monto lempiras" and "Monto dólares"  and 
    # rename Fecha to date and Concepto to description
    # Drop the columns "Monto lempiras" and "Monto dólares"
    data = data.drop(['Monto lempiras', 'Monto dólares'], axis=1)

    # Rename 'Fecha' to 'date' and 'Concepto' to 'description'
    data = data.rename(columns={'Fecha': 'date', 'Concepto': 'description'})

    # Convert the column date to date column where the date format is dd/mm/yyyy
    # and convert the amount to a decimal number with precision of 2 decimal points
    # Convert the 'date' column to a datetime object with the specified format
    data['date'] = pd.to_datetime(data['date'], format='%d/%m/%Y')

    # Convert the 'amount' column to a decimal with two decimal points
    data['amount'] = data['amount'].astype(float).round(2)

    # Multiply by -1 because expenses appear as positive
    data['amount'] = -1.0 * data['amount']

    # Add a transaction type column. Since it is a credit card in theory it is always expense
    data['tran_type'] = 'Expense'

    return data

def clean_bac_savings_stmt(dataframe: pd.DataFrame):    
    
    # Remove the empty rows.
    data = drop_null_or_empty_first_column(dataframe)

    # Rename the columns to not deal with accents
    data = data.rename(columns={'Fecha': 'date', 
                                'Descripción': 'description',
                                'Débitos': 'debitos',
                                'Créditos': 'creditos'})


    # Remove the commas and letters from the field list
    data = remove_unwanted_chars(data, ["debitos", "creditos"])

    # Convert the column date to date column where the date format is dd/mm/yyyy
    # and convert the amount to a decimal number with precision of 2 decimal points
    # Convert the 'date' column to a datetime object with the specified format
    data['date'] = pd.to_datetime(data['date'], format='%d/%m/%Y')

    # Convert the 'debitos' and 'creditos' column to a decimal with two decimal points
    data['debitos'] = data['debitos'].astype(float).round(2)
    data['creditos'] = data['creditos'].astype(float).round(2)


    # Multiply the debitos by -1 because those are withdrawals from my account
    data['debitos'] = -1.0 * data['debitos']

    # Coalesce the two columns
    data['amount'] = np.where((data['debitos'] == 0.00) | (data['debitos'].isnull()), data['creditos'], data['debitos'])
    # data['amount'] = data['debitos'].fillna(data['creditos'])

    # Drop unneeded columns
    data = data.drop(['Referencia', 'debitos', 'creditos', 'Balance'], axis=1)

    # Add a transaction type column. This simplistic logic is added here. We will address more complex issues about this later in the code
    # Right now this assumption is enough. 
    data['tran_type'] = np.where(data['amount'] < 0, 'Expense', 'Income') 

    # Add the exchange rate. Right now it is hardcoded
    data['exchange_rate'] = np.where(data["currency"] == "USD", 24.8, 1.0)

    return data

# Create a function that iterates through all the csv files in a directory
# and appends them one after the other in a dataframe. Each CSV might have different columns
# This function returns a list of dataframes where each dataframe is all the 
# csv files that had the same columns. That means that if there are 3 types of 
# CSVs there will be 3 items in the return list 

def combine_csv_files_by_columns(directory):
    dataframes_dict = {}

    # Iterate through all files in the directory
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        
        # Check if it is a file and has a .csv extension
        if os.path.isfile(filepath) and filename.endswith('.csv'):
            # Read the CSV file and obtain its columns as a tuple
            try:
                temp_df = clean_column_values(pd.read_csv(filepath, dtype=str))
                # Get the characters before the first occurrence of "(" or "." in a variable
                account_name = filename.split('.', 1)[0]
                account_name = str.strip(account_name.split('(', 1)[0])
                temp_df['account_name'] = account_name
                currency = get_account_currency(account_name)
                if currency != 'UNKNOWN':
                    temp_df['currency'] = currency

                columns_tuple = tuple(temp_df.columns)
                
                # If the columns tuple is not in the dictionary, add it with this DataFrame
                if columns_tuple not in dataframes_dict:
                    dataframes_dict[columns_tuple] = [temp_df]
                else:
                    # Otherwise, append the DataFrame to the existing entry
                    dataframes_dict[columns_tuple].append(temp_df)
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    # Combine DataFrames with the same columns
    combined_dataframes_list = []
    for _, dfs in dataframes_dict.items():
        # Concatenate all DataFrames with the same structure
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_dataframes_list.append(combined_df)

    return combined_dataframes_list

def get_account_currency(filename):
    # Define a function to act as a 'switch' based on account name
    cases = {
        'BAC USD 911': 'USD',
        'BAC HNL 271': 'HNL',
        'BAC USD 021': 'USD',
        'BAC HNL 971': 'HNL'
    }
    return cases.get(filename, 'UNKNOWN')

def raise_unknown_error(object):
    raise Exception('Found a dataframe that does not have the correct column structure.')
# Create a function that remove all whitespace characters(tabs, spaces, new lines)
# from the beginning and the end of all columns in a dataframe
# and converts all whitespaces in between words to a single space

def classify_and_transform_dataframe(df):
    # Define a mapping of DataFrame types to transformation functions
    transformation_mapping = {
        'BAC_CC': clean_bac_cc_stmt,
        'BAC_SAVINGS': clean_bac_savings_stmt,
        'UNKNOWN': raise_unknown_error
    }
    
    # Define a function to act as a 'switch' based on the column list
    def switch(columns):
        cases = {
            ('Fecha', 'Concepto', 'Monto lempiras', 'Monto dólares', 'account_name'): "BAC_CC",
            ('Fecha', 'Referencia', 'Descripción', 'Débitos', 'Créditos', 'Balance', 'account_name', 'currency'): "BAC_SAVINGS"
        }
        return cases.get(tuple(columns), 'UNKNOWN')
    
    # Get the DataFrame type
    df_type = switch(df.columns)
    
    # Call the corresponding transformation function
    return transformation_mapping[df_type](df)

def hash_row_fields(row: pd.DataFrame.row):
    # Create a hash object using sha256
    hash_object = hashlib.sha256()
    
    # Iterate over the contents of each row included in the rows_hash list and encode them. 
    # Only these columns contribute to the hash
    cols_to_hash = ['date', 'description', 'amount', 'account_name']

    for col in cols_to_hash:
        hash_object.update(str(row[col]).encode())
        
    # Return the hexadecimal digest of the hash object
    return hash_object.hexdigest()

def create_hash_for_each_row(df):
    # Apply the hash_row function across the rows of the DataFrame
    # axis=1 specifies that the function should be applied on rows rather than columns
    df['id'] = df.apply(lambda row: hash_row_fields(row), axis=1)
    df = df.set_index('id')
    return df

def clean_dataframes():
    dataframes = combine_csv_files_by_columns(".\data\\")
    clean_dataframes = []
    for dataframe in dataframes:
        data = classify_and_transform_dataframe(dataframe)
        # Add the extra columns that we need and are not populated by default
        data['vendor'] = np.nan
        data['notes'] = np.nan
        data['category'] = np.nan
        
        clean_dataframes.append(create_hash_for_each_row(data))
                
    return clean_dataframes

def hash_blank_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Takes in the existing transactions DataFrames and hashes the rows that do not have a hash already. Some entries might be 
    manually added to the table in Excel and might not have a hash yet.

    Args:
        df (pd.DataFrame): A DataFrame containing the transaction from the historical ledger,

    Returns:
        pd.DataFrame: A DataFrame where the null values in the id column has an assigned hash 
    """
    
    # This dataset is not going to get big enough for this lamda function to work all over again on rows that were already hashed. 
    # So we just do it for every row indeed.

    df = create_hash_for_each_row


    return df

# Create a function that appends all dataframes in df list. 
# Additionally, create a csv file in filepath if it doesn't exist
# already that contains the contents of the appended dataframes
# if the file already exists then read that file into a pandas dataframe 
# and insert the new rows. The key for insertion is a column called 
# id
def merge_dataframes_with_transactions(df_list, filepath):
    # Append all provided DataFrames
    appended_df = pd.concat(df_list, ignore_index=False)
    
    # Check if the CSV file exists
    if not os.path.isfile(filepath):
        # If the file does not exist, write the concatenated DataFrame to CSV
        appended_df.to_csv(filepath, index=True)
    else:
        # If the file exists, read the CSV into a DataFrame
        existing_transactions = pd.read_csv(filepath, index_col=0)

        # Some rows might not have a hash because they were manually input in the csv file
        existing_transactions = create_hash_for_each_row(existing_transactions)
        
        # Perform a left join to identify new records since the bank statement exports can contain records already in the existing
        # transactions. Add a placeholder column with a constant value to perform the join without pandas
        # adding _x and _y to the output column names because both dataframes have the same column names.
        existing_transactions.loc[:, 'placeholder'] = 1
        new_transactions = appended_df\
                .merge(existing_transactions[['placeholder']], left_index=True, right_index=True, how='left')\
                .drop(['placeholder'], axis=1)\
                .sort_values(by=['date'], ascending=False)
        
        all_transactions = pd.concat([existing_transactions, new_transactions])

        all_transactions.to_csv(filepath, index=True)

cleaned = clean_dataframes()
merge_dataframes_with_transactions(cleaned, 'out/transactions.csv')
print("stop")