import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Config:
    URL = "https://download.geonames.org/export/zip/allCountries.zip"
    SAVE_DIR = Path(os.getenv("SAVE_DIR", "data"))
    DATABASE_FILEPATH = SAVE_DIR / "geonames.db"
    ZIP_FILE = SAVE_DIR / "allCountries.zip"
    TXT_FILE = SAVE_DIR / "allCountries.txt"
    CHUNK_SIZE = 200000
