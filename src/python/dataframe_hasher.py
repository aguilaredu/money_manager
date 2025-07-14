import pandas as pd
import hashlib

class DataFrameHasher:
    def __init__(self, df, cols_to_hash, output_hash_col_name):
        """
        Initialize the hasher with a specified hashing algorithm.
        :param hash_algorithm: The algorithm to use for hashing (default is 'sha256').
        """
        self.df = df
        self.cols_to_hash = cols_to_hash
        self.output_hash_col_name = output_hash_col_name
        self.hashed_df = pd.DataFrame()
    

    def get_hashed_df(self):
        self.hash_rows()
        return self.hashed_df

    def hash_row(self, row):
    # Create a hash object using sha256
        hash_object = hashlib.sha256()
        
        # Iterate over the contents of each row included in the self.cols_to_hash list and encode them. 
        for col in self.cols_to_hash:
            hash_object.update(str(row[col]).encode())
            
        # Return the hexadecimal digest of the hash object
        return hash_object.hexdigest()

    def hash_rows(self):
        # Apply the hash_row function across the rows of the DataFrame
        # axis=1 specifies that the function should be applied on rows rather than columns
        hashed_df = self.df.copy()
        hashed_df[self.output_hash_col_name] = hashed_df.apply(lambda row: self.hash_row(row), axis=1)
        hashed_df = hashed_df.set_index(self.output_hash_col_name)
        self.hashed_df = hashed_df