class DuplicateRemover():
    def __init__(self, existing_transactions, new_transactions, match_threshold = 0.90) -> None:
        self.existing_transactions = existing_transactions
        self.new_transactions = new_transactions
        self.match_threshold = match_threshold
        self.updated_existing_transactions = None
        self.updated_new_transactions = None

    def calculate_similarity_pct(self, string1, string2):
        """Calculates the similarity percentage between two strings
        """
        pass

    def remove_duplicates(self, existing_transactions, new_transactions):
        """Iterates through the new transactions and compares them against the existing transactions.
        If a match is found the transaction is updated. If no match then the transactions gets saved
        because it will need to be inserted
        """

        # Perform iteration
        # Prefilter the existing transactions to avoid iterating through unnecessary things 
        # Calculate the similarity of the description and ensure totals match 
        # Update the transaction based on the ID 
        # Calculate the hash of the row again 
        # Store the updated_existing_transactions, store the updated_new_transactions

        pass