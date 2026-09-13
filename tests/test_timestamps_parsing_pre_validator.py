"""Tests for the TimestampsParsingPreValidator."""

import pathlib

import pytest

from s3_log_extraction.validate import TimestampsParsingPreValidator

_LOG_SUFFIX = ' - 10 10 "-" "TestAgent" - - - - - - - - -'


def _make_log_line(datetime: str, request_type: str = "REST.GET.OBJECT", status: str = "200") -> str:
    """
    Build a synthetic S3 log line with the specified bracketed datetime, request type, and status.

    Parameters
    ----------
    datetime : str
        The bracketed datetime field, e.g. ``"[01/Jan/2020:00:00:00 +0000]"``.
    request_type : str
        The operation field, e.g. ``"REST.GET.OBJECT"``.
    status : str
        The HTTP status code string, e.g. ``"200"``.

    Returns
    -------
    str
        A single S3 log line string.
    """
    return (
        f"abc123 bucket {datetime} 192.0.2.1 - REQ123 {request_type} test/file.dat "
        f'"GET /test/file.dat HTTP/1.1" {status}{_LOG_SUFFIX}\n'
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "datetime",
    [
        "[01/Jan/2020:00:00:00 +0000]",
        "[15/Jun/2021:13:45:59 +0000]",
        "[31/Dec/2022:23:59:59 -0500]",
    ],
)
def test_timestamps_parsing_valid(tmp_path: pathlib.Path, datetime: str) -> None:
    """Validator should pass when the timestamp parses to the expected 12-character form."""
    log_file = tmp_path / "valid.log"
    log_file.write_text(_make_log_line(datetime=datetime))

    validator = TimestampsParsingPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_timestamps_parsing_skips_non_get_request(tmp_path: pathlib.Path) -> None:
    """Validator should skip lines whose request type is not 'REST.GET.OBJECT', even with a bad timestamp."""
    log_file = tmp_path / "non_get.log"
    log_file.write_text(_make_log_line(datetime="[bad +0000]", request_type="REST.PUT.OBJECT"))

    validator = TimestampsParsingPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
@pytest.mark.parametrize("status", ["404", "500", "304"])
def test_timestamps_parsing_skips_non_success_status(tmp_path: pathlib.Path, status: str) -> None:
    """Validator should skip lines without a 2xx status, even with a bad timestamp."""
    log_file = tmp_path / "non_success.log"
    log_file.write_text(_make_log_line(datetime="[bad +0000]", status=status))

    validator = TimestampsParsingPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "datetime",
    [
        "[01/Xyz/2020:00:00:00 +0000]",  # Unrecognized month abbreviation
        "[truncated +0000]",  # Far too short to slice the expected pieces out of
    ],
)
def test_timestamps_parsing_aberrant(tmp_path: pathlib.Path, datetime: str) -> None:
    """Validator should raise RuntimeError when a 2xx GET line has an unparsable timestamp."""
    log_file = tmp_path / "aberrant.log"
    log_file.write_text(_make_log_line(datetime=datetime))

    validator = TimestampsParsingPreValidator()

    with pytest.raises(RuntimeError, match="Timestamps parsing pre-check failed"):
        validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_timestamps_parsing_missing_file(tmp_path: pathlib.Path) -> None:
    """Validator should raise RuntimeError when the log file does not exist."""
    validator = TimestampsParsingPreValidator()

    with pytest.raises(RuntimeError, match="Timestamps parsing pre-check failed"):
        validator._run_validation(file_path=tmp_path / "does_not_exist.log")


@pytest.mark.ai_generated
def test_timestamps_parsing_hash_is_stable() -> None:
    """The hash should be reproducible across instances since it is derived from the awk script content."""
    validator = TimestampsParsingPreValidator()
    other_validator = TimestampsParsingPreValidator()

    assert hash(validator) == hash(other_validator)
