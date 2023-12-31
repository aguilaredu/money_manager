import pandas as pd
import os
import numpy as np
import hashlib

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

import pandas as pd

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

    # Make the columns date and description the index of the dataframe
    data = data.set_index(['date'])

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

    # Convert the 'amount' column to a decimal with two decimal points
    data['debitos'] = data['debitos'].astype(float).round(2)
    data['creditos'] = data['creditos'].astype(float).round(2)


    # Multiply the debitos by -1 because those are withdrawals from my account
    data['debitos'] = -1.0 * data['debitos']

    # Coalesce the two columns
    data['amount'] = data['debitos'].fillna(data['creditos'])

    # Drop unneeded columns
    data = data.drop(['Referencia', 'debitos', 'creditos', 'Balance'], axis=1)
    data = data.set_index(['date'])

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
            ('Fecha', 'Concepto', 'Monto lempiras', 'Monto dólares'): "BAC_CC",
            ('Fecha', 'Referencia', 'Descripción', 'Débitos', 'Créditos', 'Balance'): "BAC_SAVINGS"
        }
        return cases.get(tuple(columns), 'UNKNOWN')
    
    # Get the DataFrame type
    df_type = switch(df.columns)
    
    # Call the corresponding transformation function
    return transformation_mapping[df_type](df)

def hash_row(row):
    # Create a hash object using sha256
    hash_object = hashlib.sha256()
    
    # Iterate over the items in the row and update the hash object with the
    # string representation of each item, converting it to bytes.
    for item in row:
        hash_object.update(str(item).encode())
        
    # Return the hexadecimal digest of the hash object
    return hash_object.hexdigest()

def create_hash_for_each_row(df):
    # Apply the hash_row function across the rows of the DataFrame
    # axis=1 specifies that the function should be applied on rows rather than columns
    df['id'] = df.apply(lambda row: hash_row(row), axis=1)
    return df

def clean_dataframes():
    dataframes = combine_csv_files_by_columns(".\data\\")
    clean_dataframes = []
    for dataframe in dataframes:
        data = classify_and_transform_dataframe(dataframe)
        clean_dataframes.append(create_hash_for_each_row(data))
    
    return clean_dataframes

# Create a function that creates a colission free hash for each 
# row in a dataframe based on all of its columns

cleaned = clean_dataframes()
print("stop")