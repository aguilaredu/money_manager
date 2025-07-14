class FileReadError(Exception):
    def __init__(self, message, filepath):
        super().__init__(message)
        self.filepath = filepath

    def __str__(self):
        return f"{self.args[0]} (Filepath: {self.filepath})"