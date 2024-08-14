"""
Primary functions for parsing a single line of a raw S3 log.

The strategy is to...

1) Parse the raw line into a list of strings using a regex pattern.
2) Construct a FullLogLine object from the parsed line. A collections.namedtuple object is used for performance.
3) Reduce and map the information from the FullLogLine into a ReducedLogLine object.
   This uses a lot less memory than the full version.
   Some of the mapping operations at this step include...
      - Identifying the DANDI asset ID from the full blob.
      - Parsing the timestamp in memory as a datetime.datetime object.
      - Filtering out log lines from excluded IPs (such as Drogon or GitHub actions).
      - Converting the full remote IP to a country and region, so it can be saved without violating privacy.
"""

import collections
import datetime
import importlib.metadata
import pathlib
import re
from collections.abc import Callable

from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH

# Known forms:
# REST.GET.OBJECT
# REST.PUT.OBJECT
# REST.HEAD.OBJECT
# REST.POST.OBJECT
# REST.DELETE.OBJECT
# REST.OPTIONS.PREFLIGHT
# BATCH.DELETE.OBJECT
# Longer names are truncated for lower data overhead via direct slicing based on known lengths and separator locations
_KNOWN_REQUEST_TYPES = ["GET", "PUT", "HEAD", "POST", "DELE", "OPTI", ".DEL"]

_IS_REQUEST_TYPE_KNOWN = collections.defaultdict(bool)
for request_type in _KNOWN_REQUEST_TYPES:
    _IS_REQUEST_TYPE_KNOWN[request_type] = True

_FULL_PATTERN_TO_FIELD_MAPPING = [
    "bucket_owner",
    "bucket",
    "timestamp",
    "ip_address",
    "requester",
    "request_id",
    "operation",
    "asset_id",
    "request_uri",
    # "http_version",  # Regex not splitting this from the request_uri...
    "status_code",
    "error_code",
    "bytes_sent",
    "object_size",
    "total_time",
    "turn_around_time",
    "referrer",
    "user_agent",
    "version",
    "host_id",
    "sigv",
    "cipher_suite",
    "auth_type",
    "endpoint",
    "tls_version",
    "access_point_arn",
]
_FullLogLine = collections.namedtuple("FullLogLine", _FULL_PATTERN_TO_FIELD_MAPPING)

_S3_LOG_REGEX = re.compile(pattern=r'"([^"]+)"|\[([^]]+)]|([^ ]+)')


def _append_reduced_log_line(
    *,
    raw_line: str,
    reduced_and_binned_logs: collections.defaultdict[str, dict[str, list[str | int]]],
    asset_id_handler: Callable,
    bucket: str,
    request_type: str,
    excluded_ips: collections.defaultdict[str, bool],
    line_index: int,
    log_file_path: pathlib.Path,
    task_id: str,
) -> None:
    """
    Append the `reduced_and_binned_logs` map with information extracted from a single raw log line, if it is valid.

    Parameters
    ----------
    raw_line : string
        A single line from the raw S3 log file.
    reduced_and_binned_logs : collections.defaultdict
        A map of reduced log line content binned by handled asset ID.
    asset_id_handler : callable, optional
        If your asset IDs in the raw log require custom handling (i.e., they contain slashes that you do not wish to
        translate into nested directory paths) then define a function of the following form:

        # For example
        def asset_id_handler(*, raw_asset_id: str) -> str:
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]
    bucket : string
        Only parse and return lines that match this bucket string.
    request_type : string
        The type of request to filter for.
    excluded_ips : collections.defaultdict of strings to booleans
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    line_index: int
        The index of the line in the raw log file.
    log_file_path: pathlib.Path
        The path to the log file being parsed; attached for error collection purposes.
    task_id: str
        A unique task ID to ensure that error collection files are unique when parallelizing to avoid race conditions.
    """
    parsed_log_line = _parse_s3_log_line(raw_line=raw_line)

    full_log_line = _get_full_log_line(
        parsed_log_line=parsed_log_line,
        log_file_path=log_file_path,
        line_index=line_index,
        raw_line=raw_line,
        task_id=task_id,
    )

    if full_log_line is None:
        return

    # Various early skip conditions
    if full_log_line.bucket != bucket:
        return

    # Collection some quick parsing errors if anything indicates an improper parsing
    # These might slow parsing down a bit, but could be important to ensuring accuracy
    errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    errors_folder_path.mkdir(exist_ok=True)

    dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
    date = datetime.datetime.now().strftime("%y%m%d")
    lines_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_line_errors_{task_id}.txt"

    if not full_log_line.status_code.isdigit():
        message = (
            f"Unexpected status code: '{full_log_line.status_code}' on line {line_index} of file {log_file_path}.\n\n"
        )
        with open(file=lines_errors_file_path, mode="a") as io:
            io.write(message)

    operation_slice = slice(5, 8) if full_log_line.operation[8] == "." else slice(5, 9)
    handled_request_type = full_log_line.operation[operation_slice]
    if _IS_REQUEST_TYPE_KNOWN[handled_request_type] is False:
        message = (
            f"Unexpected request type: '{handled_request_type}' handled from '{full_log_line.operation}' "
            f"on line {line_index} of file {log_file_path}.\n\n"
        )
        with open(file=lines_errors_file_path, mode="a") as io:
            io.write(message)

    timezone = full_log_line.timestamp[-5:] != "+0000"
    if timezone:
        message = f"Unexpected time shift attached to log! Have always seen '+0000', found `{timezone=}`.\n\n"
        with open(file=lines_errors_file_path, mode="a") as io:
            io.write(message)

    # More early skip conditions
    # Only accept 200-block status codes
    if full_log_line.status_code[0] != "2":
        return

    if handled_request_type != request_type:
        return

    if excluded_ips[full_log_line.ip_address] is True:
        return

    # All early skip conditions done; the line is parsed so bin the reduced information by handled asset ID
    handled_asset_id = asset_id_handler(raw_asset_id=full_log_line.asset_id)
    handled_timestamp = datetime.datetime.strptime(full_log_line.timestamp[:-6], "%d/%b/%Y:%H:%M:%S")
    handled_bytes_sent = int(full_log_line.bytes_sent) if full_log_line.bytes_sent != "-" else 0

    reduced_and_binned_logs[handled_asset_id] = reduced_and_binned_logs.get(
        handled_asset_id,
        collections.defaultdict(list),
    )
    reduced_and_binned_logs[handled_asset_id]["timestamp"].append(handled_timestamp)
    reduced_and_binned_logs[handled_asset_id]["bytes_sent"].append(handled_bytes_sent)
    reduced_and_binned_logs[handled_asset_id]["ip_address"].append(full_log_line.ip_address)
    reduced_and_binned_logs[handled_asset_id]["line_index"].append(line_index)


def _find_all_possible_substring_indices(*, string: str, substring: str) -> list[int]:
    indices = list()
    start = 0
    max_iter = 10**6
    while True and start < max_iter:
        next_index = string.find(substring, start)
        if next_index == -1:  # .find(...) was unable to locate the substring
            break
        indices.append(next_index)
        start = next_index + 1

    if start >= max_iter:
        message = (
            f"Exceeded maximum iterations in `_find_all_possible_substring_indices` on `{string=}` with `{substring=}`."
        )
        raise StopIteration(message)

    return indices


def _attempt_to_remove_quotes(*, raw_line: str, bad_parsed_line: str) -> str:
    """
    Attempt to remove bad quotes from a raw line of an S3 log file.

    These quotes are not properly escaped and are causing issues with the regex pattern.
    Various attempts to fix the regex failed, so this is the most reliable correction I could find.
    """
    starting_quotes_indices = _find_all_possible_substring_indices(string=raw_line, substring=' "')
    ending_quotes_indices = _find_all_possible_substring_indices(string=raw_line, substring='" ')

    # If even further unexpected structure, just return the bad parsed line so that the error reporter can catch it
    if len(starting_quotes_indices) == 0:  # pragma: no cover
        return bad_parsed_line
    if len(starting_quotes_indices) != len(ending_quotes_indices):  # pragma: no cover
        return bad_parsed_line

    cleaned_raw_line = raw_line[0 : starting_quotes_indices[0]]
    for counter in range(1, len(starting_quotes_indices) - 1):
        next_block = raw_line[ending_quotes_indices[counter - 1] + 2 : starting_quotes_indices[counter]]
        cleaned_raw_line += " - " + next_block
    cleaned_raw_line += " - " + raw_line[ending_quotes_indices[-1] + 2 :]

    return cleaned_raw_line


def _parse_s3_log_line(*, raw_line: str) -> list[str]:
    """
    The current method of parsing lines of an S3 log file.

    Bad lines reported in https://github.com/catalystneuro/dandi_s3_log_parser/issues/18 led to quote scrubbing
    as a pre-step. No self-contained single regex was found that could account for this uncorrected strings.
    """
    parsed_log_line = [a or b or c for a, b, c in _S3_LOG_REGEX.findall(string=raw_line)]

    number_of_parsed_items = len(parsed_log_line)

    # Everything worked as expected
    if number_of_parsed_items <= 26:
        return parsed_log_line

    potentially_cleaned_raw_line = _attempt_to_remove_quotes(raw_line=raw_line, bad_parsed_line=parsed_log_line)
    parsed_log_line = [a or b or c for a, b, c in _S3_LOG_REGEX.findall(string=potentially_cleaned_raw_line)]

    return parsed_log_line


def _get_full_log_line(
    *,
    parsed_log_line: list[str],
    log_file_path: pathlib.Path,
    line_index: int,
    raw_line: str,
    task_id: str,
) -> _FullLogLine | None:
    """Construct a FullLogLine from a single parsed log line, or dump to error collection file and return None."""
    full_log_line = None

    number_of_parsed_items = len(parsed_log_line)
    match number_of_parsed_items:
        # ARN not detected
        case 24:
            parsed_log_line.append("-")
            full_log_line = _FullLogLine(*parsed_log_line)
        # Expected length for good lines
        case 25:
            full_log_line = _FullLogLine(*parsed_log_line)
        # Happens for certain types of HEAD requests; not sure what the extra element is
        case 26:
            full_log_line = _FullLogLine(*parsed_log_line[:25])

    # Deviant log entry; usually some very ill-formed content in the URI
    # Dump information to a log file in the base folder for easy sharing
    if full_log_line is None:  # pragma: no cover
        errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
        errors_folder_path.mkdir(exist_ok=True)

        dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
        date = datetime.datetime.now().strftime("%y%m%d")
        lines_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_line_errors_{task_id}.txt"

        # TODO: automatically attempt to anonymize any detectable IP address in the raw line by replacing with 192.0.2.0
        with open(file=lines_errors_file_path, mode="a") as io:
            io.write(f"Line {line_index} of {log_file_path} (parsed {number_of_parsed_items} items): {raw_line}\n\n")

    return full_log_line
