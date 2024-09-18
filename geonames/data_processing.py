from pathlib import Path
from typing import Any, Dict, Iterator

import pandas as pd
from loguru import logger

from geonames.utils import get_column_info


def load_data_in_chunks(
    txt_filename: Path, chunksize: int = 200000
) -> Iterator[pd.DataFrame]:
    """
    Load data from a text file in chunks using pandas.

    Args:
        txt_filename (Path): The path to the text file containing the data.
        chunksize (int, optional): The number of rows to load in each chunk. Defaults to 200000.

    Returns:
        Iterator[pd.DataFrame]: An iterator of pandas DataFrames containing the loaded data.

    Raises:
        FileNotFoundError: If the specified file does not exist.
    """
    logger.info(f"Loading data from {txt_filename} in chunks of {chunksize}")

    if not txt_filename.exists():
        raise FileNotFoundError(f"File not found: {txt_filename}")

    column_info = get_column_info()
    columns, dtypes = zip(*column_info.items())

    return pd.read_csv(
        txt_filename,
        sep="\t",
        header=None,
        names=columns,
        dtype=dict(zip(columns, dtypes)),
        chunksize=chunksize,
        na_filter=False,  # Treat empty fields as empty strings instead of NaN
    )


def process_chunk(chunk: pd.DataFrame) -> Dict[str, Any]:
    """
    Process a chunk of data and convert it to a dictionary format suitable for database insertion.

    Args:
        chunk (pd.DataFrame): A pandas DataFrame containing a chunk of geonames data.

    Returns:
        Dict[str, Any]: A dictionary containing the processed data ready for database insertion.
    """
    required_columns = set(get_column_info().keys())
    if not required_columns.issubset(chunk.columns):
        logger.warning(
            f"Missing columns in chunk: {required_columns - set(chunk.columns)}"
        )
        return []

    return chunk.to_dict(orient="records")
