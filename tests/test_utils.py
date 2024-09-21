import io
import zipfile
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
from aiohttp import ClientResponseError, web
from aioresponses import aioresponses

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
async def test_download_zip(zip_server, tmp_path):
    url = f"http://localhost:{zip_server.port}/test.zip"
    test_file = tmp_path / "test.zip"

    await download_zip(url, test_file)

    assert test_file.exists(), "The downloaded zip file does not exist"
    with zipfile.ZipFile(test_file, "r") as zip_ref:
        assert "test.txt" in zip_ref.namelist(), "'test.txt' not found in the zip file"
        with zip_ref.open("test.txt") as zip_file:
            content = zip_file.read().decode("utf-8").strip()
            assert content == "This is a test file inside a zip archive."


@pytest.mark.asyncio
async def test_download_zip_network_error():
    url = "https://example.com/nonexistent.zip"
    test_file = Path("test.zip")

    with aioresponses() as m:
        m.get(url, exception=ClientResponseError(None, None, status=404))

        with pytest.raises(ClientResponseError):
            await download_zip(url, test_file)


@pytest.mark.asyncio
async def test_download_zip_403_error():
    url = "https://example.com/forbidden.zip"
    test_file = Path("test.zip")

    with aioresponses() as m:
        m.get(url, status=403)
        with pytest.raises(ValueError, match=f"Access forbidden to {url}"):
            await download_zip(url, test_file)


@pytest.mark.asyncio
async def test_extract_zip(tmp_path):
    zip_path = tmp_path / "test.zip"
    extract_dir = tmp_path / "extracted"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("test1.txt", "Test content 1")
        zf.writestr("test2.txt", "Test content 2")

    extracted_files = await extract_zip(zip_path, extract_dir)

    assert len(extracted_files) == 2
    assert "test1.txt" in extracted_files
    assert "test2.txt" in extracted_files
    assert (extract_dir / "test1.txt").read_text() == "Test content 1"
    assert (extract_dir / "test2.txt").read_text() == "Test content 2"


@pytest.mark.asyncio
async def test_extract_zip_invalid_zip(tmp_path):
    invalid_zip = tmp_path / "invalid.zip"
    invalid_zip.write_bytes(b"This is not a valid zip file")

    with pytest.raises(zipfile.BadZipFile):
        await extract_zip(invalid_zip, tmp_path / "extracted")


@pytest.mark.asyncio
async def test_extract_zip_empty_zip(tmp_path):
    empty_zip = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty_zip, "w"):
        pass

    with pytest.raises(FileNotFoundError, match="No files were extracted"):
        await extract_zip(empty_zip, tmp_path / "extracted")


@pytest.mark.asyncio
async def test_check_for_updates_file_not_exists(tmp_path):
    url = "https://example.com/test.zip"
    current_file = tmp_path / "nonexistent.zip"

    assert await check_for_updates(url, current_file) == True


@pytest.mark.asyncio
async def test_check_for_updates_different_size(tmp_path):
    url = "https://example.com/test.zip"
    current_file = tmp_path / "test.zip"
    current_file.write_bytes(b"small content")

    with aioresponses() as m:
        m.head(url, headers={"Content-Length": "1000"})
        assert await check_for_updates(url, current_file) == True


@pytest.mark.asyncio
async def test_check_for_updates_newer_remote_file(tmp_path):
    url = "https://example.com/test.zip"
    current_file = tmp_path / "test.zip"
    current_file.write_bytes(b"test content")

    old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)
    os.utime(current_file, (old_time.timestamp(), old_time.timestamp()))

    new_time = datetime(2021, 1, 1, tzinfo=timezone.utc)
    with aioresponses() as m:
        m.head(
            url,
            headers={
                "Content-Length": str(len(b"test content")),
                "Last-Modified": new_time.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            },
        )
        assert await check_for_updates(url, current_file) == True


@pytest.mark.asyncio
async def test_check_for_updates_no_update_needed(tmp_path):
    url = "https://example.com/test.zip"
    current_file = tmp_path / "test.zip"
    current_file.write_bytes(b"test content")

    current_time = datetime.now(timezone.utc)
    os.utime(current_file, (current_time.timestamp(), current_time.timestamp()))

    with aioresponses() as m:
        m.head(
            url,
            headers={
                "Content-Length": str(len(b"test content")),
                "Last-Modified": current_time.strftime("%a, %d %b %Y %H:%M:%S GMT"),
            },
        )
        assert await check_for_updates(url, current_file) == False