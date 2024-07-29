from pandas import DataFrame, to_datetime, merge
import numpy as np 
def drop_null_or_empty_rows(df: DataFrame, col_index: int) -> DataFrame:
    """
    Remove rows from the DataFrame where a specified column has null or empty values.

    Args:
        df: A pandas DataFrame.
        col_index: Index of the column to check for null or empty values.

    Returns:
        DataFrame: A new DataFrame with rows removed where the specified column contains null or empty values.
    """
    # Get the name of the column 
    first_column_name = df.columns[col_index]
    
    # Drop rows where the first column is null or empty
    return df[df[first_column_name].notnull() & (df[first_column_name].str.strip() != '')]


def remove_unwanted_chars(df: DataFrame, columns: list, pattern: str = r'[^\d.\-]') -> DataFrame:
    """
    Clean specified columns in the DataFrame by removing unwanted characters.

    Args:
        df (DataFrame): The pandas DataFrame to clean.
        columns (list or str): List of column names to clean.
        pattern (str, optional): Regular expression pattern to match unwanted characters. Defaults to r'[^\d.\-]'.

    Returns:
        DataFrame: A new pandas DataFrame with specified columns converted to numeric.
    """

    # Create a copy of the DataFrame to avoid modifying the original (immutability principle)
    cleaned_df = df.copy()

    # Loop through each column and clean it
    for column in columns:
        cleaned_df.loc[:, column] = cleaned_df[column].astype(str).str.replace(pattern, '', regex=True)

    return cleaned_df

def replace_empty_string_with_nan(df: DataFrame, columns: list) -> DataFrame:
    """
    Replace empty strings with NaN in specified columns of a DataFrame.

    Args:
        df (DataFrame): The DataFrame in which to replace empty strings.
        columns (list[str]): The list of columns to check for empty strings.

    Returns:
        DataFrame: A copy of the DataFrame with empty strings replaced by NaN in the specified columns.
    """
    
    df_copy = df.copy()
    for column in columns:
        df_copy[column] = df_copy[column].replace(r'^\s*$', np.nan, regex=True)

    return df_copy

def clean_column_values(df):
    # Clean all string columns in a DataFrame
    
    # Define a lambda function that replaces multiple whitespaces with a single space
    # and strips leading and trailing whitespaces
    clean_str = lambda x: ' '.join(str(x).split()) if isinstance(x, str) else x
    
    # Apply this function to every element of the DataFrame (assuming it's a string)
    return df.applymap(clean_str)

def fill_missing_exchange_rates(dataframe, date_col, exchange_rate_dataframe, correction_factor = 1.02):

    if exchange_rate_dataframe is None:
        return dataframe

    # Rename the exchange rate column in the new dataframe to prevent column duplication
    exchange_rate_dataframe = exchange_rate_dataframe.rename("exchange_rate_new")

    out_df = (merge(dataframe, exchange_rate_dataframe, left_on=[date_col], right_index=True, how='left')
              .assign(exchange_rate_new=lambda df: df['exchange_rate_new'].astype(float)*correction_factor)
              .assign(exchange_rate=lambda df: np.where(df["exchange_rate"].isna(), df["exchange_rate_new"], 
                                       df["exchange_rate"]))
              .assign(exchange_rate=lambda df: df["exchange_rate"].fillna(method='ffill'))
              .assign(exchange_rate=lambda df: df['exchange_rate'].astype(float).round(2))
              .drop(['exchange_rate_new'], axis=1))
    

    return out_df