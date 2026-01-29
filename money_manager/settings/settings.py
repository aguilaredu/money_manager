from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Use Path objects instead of strings for better cross-platform compatibility
    out_file_path: Path = Path("data/out/transactions.csv")
    in_folder_path: Path = Path("data/in/")


# Global instance to be imported elsewhere
GLOBAL_SETTINGS = Settings()
