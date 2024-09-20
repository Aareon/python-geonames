from pathlib import Path
from typing import Any, Dict, Generator, List

import pandas as pd
from loguru import logger


def get_column_info() -> Dict[str, Any]:
    return {
        "country_code": str,
        "postal_code": str,
        "place_name": str,
        "admin_name1": str,
        "admin_code1": str,
        "admin_name2": str,
        "admin_code2": str,
        "admin_name3": str,
        "admin_code3": str,
        "latitude": float,
        "longitude": float,
        "accuracy": float,
    }


def load_data_in_chunks(
    txt_filename: Path, chunksize: int = 200000
) -> Generator[pd.DataFrame, None, None]:
    logger.debug(f"Loading data from {txt_filename} in chunks of {chunksize}")

    if not txt_filename.exists():
        raise FileNotFoundError(f"File not found: {txt_filename}")

    column_info = get_column_info()
    columns = list(column_info.keys())

    # Custom converter for float columns
    float_converter = lambda x: pd.to_numeric(x, errors="coerce")

    converters = {
        "latitude": float_converter,
        "longitude": float_converter,
        "accuracy": float_converter,
    }

    dtypes = {col: str for col in columns if col not in converters}

    for chunk in pd.read_csv(
        txt_filename,
        sep="\t",
        header=None,
        names=columns,
        dtype=dtypes,
        converters=converters,
        chunksize=chunksize,
        na_values=[""],
        keep_default_na=False,
        encoding="utf-8",
        on_bad_lines="skip",
        low_memory=False,
    ):
        logger.debug(f"Loaded chunk with {len(chunk)} rows")
        yield chunk


def process_chunk(chunk: pd.DataFrame) -> List[Dict[str, Any]]:
    logger.debug(f"Processing chunk of size {len(chunk)}")
    required_columns = set(get_column_info().keys())
    if not required_columns.issubset(chunk.columns):
        logger.warning(
            f"Missing columns in chunk: {required_columns - set(chunk.columns)}"
        )
        return []

    # Convert non-float columns to their specified types
    for col, dtype in get_column_info().items():
        if dtype != float:
            chunk[col] = chunk[col].astype(dtype)

    # Drop rows with NaN values in required numeric columns
    chunk = chunk.dropna(subset=["latitude", "longitude"])

    result: List[Dict[str, Any]] = chunk.to_dict(orient="records")
    logger.debug(f"Processed {len(result)} records from chunk")
    return result
