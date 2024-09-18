import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pytest
from aiohttp import web
from aioresponses import aioresponses

from geonames.config import Config
from geonames.utils import check_for_updates, download_zip, extract_zip


@pytest.fixture
async def zip_server(aiohttp_server):
    async def handle_zip(request):
        content = "This is a test file inside a zip archive."
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("test.txt", content)
        return web.Response(
            body=zip_buffer.getvalue(), headers={"Content-Type": "application/zip"}
        )

    app = web.Application()
    app.router.add_get("/test.zip", handle_zip)
    return await aiohttp_server(app)


@pytest.mark.asyncio
async def test_download_zip(zip_server):
    url = f"http://localhost:{zip_server.port}/test.zip"
    test_file = Path("test.zip")

    try:
        await download_zip(url, test_file)

        assert test_file.exists(), "The downloaded zip file does not exist"

        with zipfile.ZipFile(test_file, "r") as zip_ref:
            file_list = zip_ref.namelist()
            assert "test.txt" in file_list, "'test.txt' not found in the zip file"

            expected_content = "This is a test file inside a zip archive."
            with zip_ref.open("test.txt") as zip_file:
                content = zip_file.read().decode("utf-8").strip()
                assert (
                    content == expected_content
                ), "File content does not match expected content"

    finally:
        if test_file.exists():
            test_file.unlink()


@pytest.mark.asyncio
async def test_download_zip_403_error():
    url = "https://example.com/forbidden.zip"
    test_file = Path("test.zip")

    with aioresponses() as m:
        m.get(url, status=403)
        with pytest.raises(ValueError, match=f"Access forbidden to {url}"):
            await download_zip(url, test_file)


@pytest.mark.asyncio
async def test_check_for_updates(mocker):
    config = Config()
    url = config.URL
    zip_file = config.ZIP_FILE

    # Mock the file system
    mock_stat = mocker.Mock()
    mock_stat.st_size = 900
    mock_stat.st_mtime = datetime(2015, 10, 21, 7, 28, tzinfo=timezone.utc).timestamp()
    mocker.patch("pathlib.Path.stat", return_value=mock_stat)
    mocker.patch("pathlib.Path.exists", return_value=True)

    with aioresponses() as m:
        # Mock the HTTP response for both calls
        m.head(
            url,
            status=200,
            headers={
                "Content-Length": "1000",
                "Last-Modified": "Wed, 21 Oct 2015 07:29:00 GMT",
            },
            repeat=True,
        )

        # Test when update is needed
        result = await check_for_updates(url, zip_file)
        assert result == True, "Expected an update to be needed"

        # Test when file is up to date
        mock_stat.st_size = 1000
        mock_stat.st_mtime = datetime(
            2015, 10, 21, 7, 29, tzinfo=timezone.utc
        ).timestamp()
        result = await check_for_updates(url, zip_file)
        assert result == False, "Expected no update to be needed"


@pytest.mark.asyncio
async def test_extract_zip(tmp_path):
    # Create a test zip file
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("test.txt", "Test content")

    # Extract the zip file
    extract_dir = tmp_path / "extracted"
    await extract_zip(zip_path, extract_dir)

    # Check if the file was extracted correctly
    extracted_file = extract_dir / "test.txt"
    assert extracted_file.exists(), "Extracted file does not exist"
    assert (
        extracted_file.read_text() == "Test content"
    ), "Extracted content does not match"
