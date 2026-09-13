"""Tests for the record keeping, resumption, and stop handling of the local extractor."""

import pathlib

import pytest

from s3_log_extraction.extractors import S3LogAccessExtractor

_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"


def _write_records(cache_directory: pathlib.Path, *, started: list[str], ended: list[str]) -> None:
    """Seed the file-processing records of a cache directory as a previous run would have left them."""
    records_directory = cache_directory / "records"
    records_directory.mkdir(parents=True, exist_ok=True)
    start_file_path = records_directory / "S3LogAccessExtractor_file-processing-start.txt"
    end_file_path = records_directory / "S3LogAccessExtractor_file-processing-end.txt"
    start_file_path.write_text("".join(f"{line}\n" for line in started))
    end_file_path.write_text("".join(f"{line}\n" for line in ended))


@pytest.mark.ai_generated
def test_extractor_creates_its_cache_layout(tmp_path: pathlib.Path) -> None:
    """Instantiating the extractor should create the extraction and records subdirectories."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)

    assert extractor.extraction_directory == tmp_path / "extraction"
    assert extractor.extraction_directory.is_dir() is True
    assert extractor.records_directory == tmp_path / "records"
    assert extractor.records_directory.is_dir() is True
    assert extractor.file_processing_end_record == set()


@pytest.mark.ai_generated
def test_extractor_reloads_completed_record(tmp_path: pathlib.Path) -> None:
    """A consistent pair of records from a previous run should be reloaded as the completed set."""
    _write_records(tmp_path, started=["log_a", "log_b"], ended=["log_a", "log_b"])

    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)

    assert extractor.file_processing_end_record == {"log_a", "log_b"}


@pytest.mark.ai_generated
def test_extractor_detects_record_corruption(tmp_path: pathlib.Path) -> None:
    """A log that started but never finished means the extraction cache is unreliable and must be reset."""
    _write_records(tmp_path, started=["log_a", "log_b"], ended=["log_a"])

    with pytest.raises(ValueError, match="Record corruption from previous run detected"):
        S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)


@pytest.mark.ai_generated
def test_extract_file_skips_already_extracted(tmp_path: pathlib.Path) -> None:
    """A log already in the completed record should not be re-extracted or re-recorded."""
    log_file = next(iter(_EXAMPLE_LOGS_DIRECTORY.iterdir()))
    _write_records(tmp_path, started=[str(log_file.absolute())], ended=[str(log_file.absolute())])

    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.extract_file(file_path=log_file)

    assert list(extractor.extraction_directory.rglob(pattern="*.txt")) == []
    assert extractor.file_processing_start_record_file_path.read_text().splitlines() == [str(log_file.absolute())]


@pytest.mark.ai_generated
def test_extract_file_honors_the_stop_file(tmp_path: pathlib.Path) -> None:
    """With the stop file present, extraction should return without touching the records."""
    log_file = next(iter(_EXAMPLE_LOGS_DIRECTORY.iterdir()))

    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.stop_file_path.touch()
    extractor.extract_file(file_path=log_file)

    assert extractor.file_processing_start_record_file_path.exists() is False
    assert extractor.file_processing_end_record == set()


@pytest.mark.ai_generated
def test_extract_file_ignores_the_stop_file_when_disabled(tmp_path: pathlib.Path) -> None:
    """Child processes pass `enable_stop=False` since the parent handles the stop signal for the batch."""
    log_file = next(iter(_EXAMPLE_LOGS_DIRECTORY.iterdir()))

    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.stop_file_path.touch()
    extractor.extract_file(file_path=log_file, enable_stop=False)

    assert extractor.file_processing_end_record == {str(log_file.absolute())}


@pytest.mark.ai_generated
def test_extract_file_records_paths_relative_to_the_log_root(tmp_path: pathlib.Path) -> None:
    """Passing a log root should record the path relative to it so that records survive a relocated log directory."""
    log_file = next(iter(_EXAMPLE_LOGS_DIRECTORY.iterdir()))

    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.extract_file(file_path=log_file, log_root=_EXAMPLE_LOGS_DIRECTORY)

    assert extractor.file_processing_end_record == {log_file.name}
    assert extractor.file_processing_end_record_file_path.read_text().splitlines() == [log_file.name]


@pytest.mark.ai_generated
def test_extract_directory_resumes_from_the_record(tmp_path: pathlib.Path) -> None:
    """A second pass over the same directory should find nothing left to extract."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, workers=1)
    number_of_extracted_logs = len(extractor.file_processing_end_record)

    resumed_extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    resumed_extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, workers=1)

    recorded_lines = resumed_extractor.file_processing_end_record_file_path.read_text().splitlines()

    assert number_of_extracted_logs == len(list(_EXAMPLE_LOGS_DIRECTORY.iterdir()))
    assert resumed_extractor.file_processing_end_record == extractor.file_processing_end_record
    assert sorted(recorded_lines) == sorted(extractor.file_processing_end_record)  # No duplicated entries


@pytest.mark.ai_generated
@pytest.mark.parametrize("limit", [1, 2])
def test_extract_directory_respects_limit(tmp_path: pathlib.Path, limit: int) -> None:
    """Only up to `limit` logs should be extracted per call."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, limit=limit, workers=1)

    assert len(extractor.file_processing_end_record) == limit


@pytest.mark.ai_generated
def test_extract_directory_stops_when_signalled(tmp_path: pathlib.Path) -> None:
    """With the stop file already present, no log should be extracted."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.stop_file_path.touch()
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, workers=1)

    assert extractor.file_processing_end_record == set()
    assert list(extractor.extraction_directory.rglob(pattern="*.txt")) == []


@pytest.mark.ai_generated
def test_extract_directory_stops_when_signalled_in_parallel(tmp_path: pathlib.Path) -> None:
    """The stop file should also end the batched, multi-worker path before any batch is submitted."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    extractor.stop_file_path.touch()
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, workers=2, batch_size=1)

    assert extractor.file_processing_end_record == set()
    assert list(extractor.extraction_directory.rglob(pattern="*.txt")) == []
