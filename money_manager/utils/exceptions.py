from typing import override


class FileReadError(Exception):
    def __init__(self, message: str, filepath: str):
        super().__init__(message)
        self.filepath: str = filepath

    @override
    def __str__(self):
        return f"{self.args[0]} (Filepath: {self.filepath})"
