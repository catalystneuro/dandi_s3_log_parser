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
import pathlib
import re
import importlib.metadata

from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH
from ._ip_utils import _get_region_from_ip_address

FULL_PATTERN_TO_FIELD_MAPPING = [
    "bucket_owner",
    "bucket",
    "timestamp",
    "remote_ip",
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
    "extra",  # TODO: Never figured out what this field is...
]
REDUCED_PATTERN_TO_FIELD_MAPPING = ["asset_id", "timestamp", "bytes_sent", "region"]

FullLogLine = collections.namedtuple("FullLogLine", FULL_PATTERN_TO_FIELD_MAPPING)
ReducedLogLine = collections.namedtuple("ReducedLogLine", REDUCED_PATTERN_TO_FIELD_MAPPING)


# Original
# S3_LOG_REGEX = re.compile(r'(?:"([^"]+)")|(?:\[([^\]]+)\])|([^ ]+)')


# AI corrected...
S3_LOG_REGEX = re.compile(r'"([^"]+)"|\[([^]]+)]|([^ ]+)')


def _parse_s3_log_line(*, raw_line: str) -> list[str]:
    """The current method of parsing lines of an S3 log file."""
    parsed_log_line = [a or b or c for a, b, c in S3_LOG_REGEX.findall(raw_line)]

    return parsed_log_line


def _get_full_log_line(
    *,
    parsed_log_line: list[str],
    log_file_path: pathlib.Path,
    index: int,
    raw_line: str,
) -> FullLogLine | None:
    """Construct a FullLogLine from a single parsed log line, or dump to error collection file and return None."""
    full_log_line = None

    number_of_parsed_items = len(parsed_log_line)
    match number_of_parsed_items:
        # ARN not detected
        case 24:
            parsed_log_line.append("-")
            parsed_log_line.append("-")
            full_log_line = FullLogLine(*parsed_log_line)
        # Expected form most of the time
        case 25:
            parsed_log_line.append("-")
            full_log_line = FullLogLine(*parsed_log_line)
        # Happens for certain types of HEAD requests
        case 26:
            full_log_line = FullLogLine(*parsed_log_line)

    # Deviant log entry; usually some very ill-formed content in the URI
    # Dump information to a log file in the base folder for easy sharing
    if full_log_line is None:
        errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
        errors_folder_path.mkdir(exist_ok=True)

        dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
        date = datetime.datetime.now().strftime("%y%m%d")
        lines_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_lines_errors.txt"

        with open(file=lines_errors_file_path, mode="a") as io:
            io.write(f"Line {index} of {log_file_path} (parsed {number_of_parsed_items} items): {raw_line}\n\n")

    return full_log_line


def _append_reduced_log_line(
    *,
    raw_line: str,
    reduced_log_lines: list[ReducedLogLine],
    bucket: str,
    request_type: str,
    excluded_ips: collections.defaultdict[str, bool],
    log_file_path: pathlib.Path,
    index: int,
    ip_hash_to_region: dict[str, str],
) -> None:
    """
    Append the `reduced_log_lines` list with a ReducedLogLine constructed from a single raw log line, if it is valid.

    Parameters
    ----------
    raw_line : string
        A single line from the raw S3 log file.
    reduced_log_lines : list of ReducedLogLine
        The list of ReducedLogLine objects to mutate in place.
        This is done to reduce overhead of copying/returning items in-memory via a return-based approach.
    bucket : string
        Only parse and return lines that match this bucket string.
    request_type : string
        The type of request to filter for.
    excluded_ips : collections.defaultdict of strings to booleans
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    """
    bucket = "" if bucket is None else bucket
    excluded_ips = excluded_ips or collections.defaultdict(bool)

    parsed_log_line = _parse_s3_log_line(raw_line=raw_line)

    full_log_line = _get_full_log_line(
        parsed_log_line=parsed_log_line,
        log_file_path=log_file_path,
        index=index,
        raw_line=raw_line,
    )

    if full_log_line is None:
        return None

    # Various early skip conditions
    if full_log_line.bucket != bucket:
        return None

    # Skip all non-success status codes (those in the 200 block)
    if full_log_line.status_code[0] != "2":
        return None

    # Derived from command string, e.g., "HEAD /blobs/b38/..."
    # Subset first 7 characters for performance
    parsed_request_type = full_log_line.request_uri[:4].removesuffix(" ")
    if parsed_request_type != request_type:
        return None

    if excluded_ips[full_log_line.remote_ip]:
        return None

    assert (
        full_log_line.timestamp[-5:] == "+0000"
    ), f"Unexpected time shift attached to log! Have always seen '+0000', found '{full_log_line.timestamp[-5:]}'."

    parsed_timestamp = datetime.datetime.strptime(full_log_line.timestamp[:-6], "%d/%b/%Y:%H:%M:%S")
    parsed_bytes_sent = int(full_log_line.bytes_sent) if full_log_line.bytes_sent != "-" else 0
    region = _get_region_from_ip_address(ip_hash_to_region=ip_hash_to_region, ip_address=full_log_line.remote_ip)
    reduced_log_line = ReducedLogLine(
        asset_id=full_log_line.asset_id,
        timestamp=parsed_timestamp,
        bytes_sent=parsed_bytes_sent,
        region=region,
    )

    reduced_log_lines.append(reduced_log_line)
