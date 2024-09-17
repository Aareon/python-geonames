import aiohttp
import aiofiles
import zipfile
from pathlib import Path
from loguru import logger
from typing import Dict, Any
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone

async def download_zip(url: str, filename: Path) -> None:
    """
    Download a zip file from the given URL and save it to the specified filename.

    Args:
        url (str): The URL to download the zip file from.
        filename (Path): The path where the downloaded file will be saved.

    Raises:
        aiohttp.ClientError: If the HTTP request fails.
        ValueError: If access is forbidden (403 error) or if the downloaded file is not a valid zip file.
    """
    logger.info(f"Downloading {filename} from {url}")
    filename.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            if response.status == 403:
                logger.error(f"Access forbidden to {url}. Server returned 403 error.")
                raise ValueError(f"Access forbidden to {url}")
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))

            async with aiofiles.open(filename, mode='wb') as f:
                downloaded = 0
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
                    downloaded += len(chunk)
                    logger.info(f"Downloaded {downloaded}/{total_size} bytes")

    logger.info(f"Downloaded {filename}")

async def extract_zip(zip_filename: Path, extract_to: Path) -> None:
    """
    Extract the contents of a zip file to the specified directory.

    Args:
        zip_filename (Path): The path to the zip file.
        extract_to (Path): The directory to extract the contents to.
    """
    logger.info(f"Extracting {zip_filename}")
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    logger.info(f"Extracted {zip_filename}")

async def check_for_updates(url: str, current_file: Path) -> bool:
    if not current_file.exists():
        return True

    async with aiohttp.ClientSession() as session:
        async with session.head(url) as response:
            response.raise_for_status()
            remote_size = int(response.headers.get("Content-Length", "0"))
            remote_modified = response.headers.get("Last-Modified")

    local_size = current_file.stat().st_size
    local_modified = datetime.fromtimestamp(current_file.stat().st_mtime, tz=timezone.utc)

    if remote_size != local_size:
        return True

    if remote_modified:
        remote_modified_datetime = parsedate_to_datetime(remote_modified)
        logger.debug(f"Remote modified: {remote_modified_datetime}, Local modified: {local_modified}")
        if remote_modified_datetime > local_modified:
            return True

    return False

def get_column_info() -> Dict[str, Any]:
    """
    Get the column information for the geonames database.

    Returns:
        Dict[str, Any]: A dictionary containing column names and their data types.
    """
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