"""Tests for the HttpEmptySplitPreValidator."""

import pathlib

import pytest

from s3_log_extraction.validate import HttpEmptySplitPreValidator

# A minimal but complete S3 log line prefix/suffix template:
#   <hash> <bucket> [<datetime>] <ip> <iam> <req_id> <operation> <key>
#   "<METHOD> /<key> HTTP/1.x" <status> <error> <bytes_sent> <total_bytes> ...
_LOG_PREFIX = "abc123 bucket [01/Jan/2020:00:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT test/file.dat "
_LOG_SUFFIX = ' 200 - 10 10 "-" "TestAgent" - - - - - - - - -'


@pytest.mark.ai_generated
@pytest.mark.parametrize("http_version", ["HTTP/1.0", "HTTP/1.1"])
def test_http_empty_split_get_request(tmp_path: pathlib.Path, http_version: str) -> None:
    """Validator should pass when a GET request line splits on the HTTP pattern."""
    log_file = tmp_path / "get.log"
    log_file.write_text(f'{_LOG_PREFIX}"GET /test/file.dat {http_version}"{_LOG_SUFFIX}\n')

    validator = HttpEmptySplitPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_empty_split_non_get_request(tmp_path: pathlib.Path) -> None:
    """Validator should pass for request types other than 'REST.GET.OBJECT'."""
    log_file = tmp_path / "head.log"
    log_file.write_text(
        "abc123 bucket [01/Jan/2020:00:00:00 +0000] 192.0.2.1 - REQ123 REST.HEAD.BUCKET - "
        '"HEAD / HTTP/1.1" 200 - 0 0 "-" "TestAgent" -\n'
    )

    validator = HttpEmptySplitPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_empty_split_blank_lines(tmp_path: pathlib.Path) -> None:
    """Validator should pass on blank lines, which split to no fields but carry no request type."""
    log_file = tmp_path / "blank.log"
    log_file.write_text(f'\n{_LOG_PREFIX}"GET /test/file.dat HTTP/1.1"{_LOG_SUFFIX}\n\n')

    validator = HttpEmptySplitPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_empty_split_missing_file(tmp_path: pathlib.Path) -> None:
    """Validator should raise RuntimeError when the log file does not exist."""
    validator = HttpEmptySplitPreValidator()

    with pytest.raises(RuntimeError, match="HTTP empty split pre-check failed"):
        validator._run_validation(file_path=tmp_path / "does_not_exist.log")


@pytest.mark.ai_generated
def test_http_empty_split_hash_is_stable() -> None:
    """The hash should be reproducible across instances since it is derived from the awk script content."""
    validator = HttpEmptySplitPreValidator()
    other_validator = HttpEmptySplitPreValidator()

    assert hash(validator) == hash(other_validator)
