"""Tests for the HttpSplitCountPreValidator."""

import pathlib

import pytest

from s3_log_extraction.validate import HttpSplitCountPreValidator

# A minimal but complete S3 log line prefix/suffix template:
#   <hash> <bucket> [<datetime>] <ip> <iam> <req_id> <operation> <key>
#   "<METHOD> /<key> HTTP/1.x" <status> <error> <bytes_sent> <total_bytes> ...
_LOG_PREFIX = "abc123 bucket [01/Jan/2020:00:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT test/file.dat "
_LOG_SUFFIX = ' 200 - 10 10 "-" "TestAgent" - - - - - - - - -'


@pytest.mark.ai_generated
@pytest.mark.parametrize("http_version", ["HTTP/1.0", "HTTP/1.1"])
def test_http_split_count_single_occurrence(tmp_path: pathlib.Path, http_version: str) -> None:
    """Validator should pass when 'HTTP/1.' occurs exactly once on a line."""
    log_file = tmp_path / "single.log"
    log_file.write_text(f'{_LOG_PREFIX}"GET /test/file.dat {http_version}"{_LOG_SUFFIX}\n')

    validator = HttpSplitCountPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_split_count_no_occurrence(tmp_path: pathlib.Path) -> None:
    """Validator should pass when 'HTTP/1.' does not occur at all, which the empty split check covers instead."""
    log_file = tmp_path / "none.log"
    log_file.write_text("abc123 bucket [01/Jan/2020:00:00:00 +0000] 192.0.2.1 - REQ123 REST.HEAD.BUCKET - - - -\n")

    validator = HttpSplitCountPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_split_count_empty_file(tmp_path: pathlib.Path) -> None:
    """Validator should pass on an empty log file."""
    log_file = tmp_path / "empty.log"
    log_file.write_text("")

    validator = HttpSplitCountPreValidator()

    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_split_count_multiple_occurrences(tmp_path: pathlib.Path) -> None:
    """Validator should raise RuntimeError when 'HTTP/1.' occurs more than once on a line.

    A user agent that embeds the HTTP pattern is the realistic way a second occurrence sneaks in.
    """
    log_file = tmp_path / "aberrant.log"
    log_file.write_text(
        f'{_LOG_PREFIX}"GET /test/file.dat HTTP/1.1" 200 - 10 10 "-" "Agent HTTP/1.1" - - - - - - - - -\n'
    )

    validator = HttpSplitCountPreValidator()

    with pytest.raises(RuntimeError, match="HTTP split count pre-check failed"):
        validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_http_split_count_missing_file(tmp_path: pathlib.Path) -> None:
    """Validator should raise RuntimeError when the log file does not exist."""
    validator = HttpSplitCountPreValidator()

    with pytest.raises(RuntimeError, match="HTTP split count pre-check failed"):
        validator._run_validation(file_path=tmp_path / "does_not_exist.log")


@pytest.mark.ai_generated
def test_http_split_count_hash_is_stable() -> None:
    """The hash should be reproducible across instances since it is derived from the awk script content."""
    validator = HttpSplitCountPreValidator()
    other_validator = HttpSplitCountPreValidator()

    assert hash(validator) == hash(other_validator)
