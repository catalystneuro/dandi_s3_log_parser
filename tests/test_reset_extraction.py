"""Tests for clearing the extraction cache and its related records."""

import pathlib

import pytest

from s3_log_extraction.config import reset_extraction


@pytest.fixture
def populated_cache_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """A cache directory holding extraction output alongside both record types."""
    cache_directory = tmp_path / "cache"
    extraction_directory = cache_directory / "extraction"
    nested_directory = extraction_directory / "dandiset" / "asset"
    nested_directory.mkdir(parents=True)
    (extraction_directory / "top_level.txt").write_text("content\n")
    (nested_directory / "timestamps.txt").write_text("content\n")

    records_directory = cache_directory / "records"
    records_directory.mkdir()
    (records_directory / "S3LogAccessExtractor_extraction.log").write_text("content\n")
    (records_directory / "S3LogAccessExtractor_file-processing-start.txt").write_text("content\n")
    (records_directory / "S3LogAccessExtractor_file-processing-end.txt").write_text("content\n")
    (records_directory / "DownloadsLogicPreValidator_abc123.txt").write_text("content\n")

    return cache_directory


@pytest.mark.ai_generated
def test_reset_extraction_clears_extraction_directory(populated_cache_directory: pathlib.Path) -> None:
    """The extraction directory should be emptied but still exist afterwards."""
    extraction_directory = populated_cache_directory / "extraction"

    reset_extraction(cache_directory=populated_cache_directory)

    assert extraction_directory.is_dir() is True
    assert list(extraction_directory.rglob(pattern="*")) == []


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "record_file_name",
    [
        "S3LogAccessExtractor_extraction.log",
        "S3LogAccessExtractor_file-processing-start.txt",
        "S3LogAccessExtractor_file-processing-end.txt",
    ],
)
def test_reset_extraction_removes_extraction_records(
    populated_cache_directory: pathlib.Path, record_file_name: str
) -> None:
    """Extraction logs and file-processing records should be removed."""
    reset_extraction(cache_directory=populated_cache_directory)

    assert (populated_cache_directory / "records" / record_file_name).exists() is False


@pytest.mark.ai_generated
def test_reset_extraction_keeps_validation_records(populated_cache_directory: pathlib.Path) -> None:
    """Validator records are unrelated to extraction and should survive the reset."""
    reset_extraction(cache_directory=populated_cache_directory)

    assert (populated_cache_directory / "records" / "DownloadsLogicPreValidator_abc123.txt").exists() is True


@pytest.mark.ai_generated
def test_reset_extraction_on_empty_cache(tmp_path: pathlib.Path) -> None:
    """Resetting a cache directory that has neither extraction output nor records should still succeed."""
    cache_directory = tmp_path / "empty_cache"
    cache_directory.mkdir()

    reset_extraction(cache_directory=cache_directory)

    assert (cache_directory / "extraction").is_dir() is True
    assert (cache_directory / "records").is_dir() is True
