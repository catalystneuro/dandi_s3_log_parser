"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import datetime
import traceback
import uuid
from collections.abc import Callable
from typing import Literal

import tqdm
from pydantic import FilePath, validate_call

from ._buffered_text_reader import BufferedTextReader
from ._error_collection import _collect_error
from ._globals import _IS_OPERATION_TYPE_KNOWN, _KNOWN_OPERATION_TYPES, _S3_LOG_FIELDS
from ._s3_log_line_parser import _get_full_log_line, _parse_s3_log_line


@validate_call
def reduce_raw_s3_log(
    *,
    raw_s3_log_file_path: FilePath,
    reduced_s3_log_file_path: FilePath,
    fields_to_reduce: list[Literal[_S3_LOG_FIELDS]] | None = None,
    object_key_parents_to_reduce: list[str] | None = None,
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    operation_type: Literal[_KNOWN_OPERATION_TYPES] = "REST.GET.OBJECT",
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    object_key_handler: Callable | None = None,
    line_buffer_tqdm_kwargs: dict | None = None,
) -> None:
    """
    Reduce a raw S3 log file to only the requested fields.

    'Reduce' here means:
      - Filtering all lines only by the type of operation specified (i.e., REST.GET.OBJECT, REST.PUT.OBJECT, etc.).
      - Filtering out any non-success status codes.
      - Filtering out any excluded IP addresses.
      - Extracting only the asset ID, request timestamp, request size, and IP address that sent the request.

    Parameters
    ----------
    raw_s3_log_file_path : file path
        The path to the raw S3 log file.
    reduced_s3_log_file_path : file path
        The path to write each reduced S3 log file to.
    fields_to_reduce : list of S3 log fields, optional
        The S3 log fields to reduce the raw log file to.
        Defaults to ["object_key", "timestamp", "bytes_sent", "ip_address"].
    object_key_parents_to_reduce : list of strings, optional
        The parent directories of the object key to reduce the raw log file to.
    maximum_buffer_size_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to use on each buffer iteration when reading from the
        source text file.

        Actual RAM usage will be higher due to overhead and caching.
    operation_type : str, default: "REST.GET"
        The type of operation to filter for.
    excluded_ips : collections.defaultdict of strings to booleans, optional
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    object_key_handler : callable, optional
        If your object keys in the raw log require custom handling (i.e., they contain slashes that you do not wish to
        translate into nested directory paths) then define and pass a function that takes the `object_key` as a string
        and returns the corrected form.

        For example:

        ```python
        def object_key_handler(*, object_key: str) -> str:
            split_by_slash = object_key.split("/")

            object_type = split_by_slash[0]
            if object_type == "zarr":
                zarr_blob_form = "/".join(split_by_slash[:2])
                return zarr_blob_form

            return object_key
        ```
    line_buffer_tqdm_kwargs : dict, optional
        Keyword arguments to pass to the tqdm progress bar for line buffers.
    """
    fields_to_reduce = fields_to_reduce or ["object_key", "timestamp", "bytes_sent", "ip_address"]
    object_key_parents_to_reduce = object_key_parents_to_reduce or []  # ["blobs", "zarr"] # TODO: move to DANDI side
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    object_key_handler = object_key_handler or (lambda object_key: object_key)
    line_buffer_tqdm_kwargs = line_buffer_tqdm_kwargs or dict()

    default_tqdm_kwargs = {"desc": "Parsing line buffers...", "leave": False}
    resolved_tqdm_kwargs = {**default_tqdm_kwargs}
    resolved_tqdm_kwargs.update(line_buffer_tqdm_kwargs)

    assert raw_s3_log_file_path.suffix == ".log", f"`{raw_s3_log_file_path=}` should end in '.log'!"

    if set(fields_to_reduce) == {"object_key", "timestamp", "bytes_sent", "ip_address"}:
        raise NotImplementedError("This function is not yet generalized for custom field reduction.")

    buffered_text_reader = BufferedTextReader(
        file_path=raw_s3_log_file_path,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )
    progress_bar_iterator = tqdm.tqdm(
        iterable=buffered_text_reader,
        total=len(buffered_text_reader),
        **resolved_tqdm_kwargs,
    )

    task_id = str(uuid.uuid4())[:5]
    fast_fields_to_reduce = set(fields_to_reduce) == {"object_key", "timestamp", "bytes_sent", "ip_address"}
    fast_object_key_parents_to_reduce = set(object_key_parents_to_reduce) == {
        "blobs",
        "zarr",
    }  # Admittedly, this is particular to DANDI
    fast_fields_case = fast_fields_to_reduce and fast_object_key_parents_to_reduce
    if fast_fields_case is True:
        reduced_s3_log_lines = [
            reduced_s3_log_line
            for raw_s3_log_lines_buffer in progress_bar_iterator
            for raw_s3_log_line in raw_s3_log_lines_buffer
            if (
                reduced_s3_log_line := _fast_dandi_reduce_raw_s3_log_line(
                    raw_s3_log_line=raw_s3_log_line,
                    operation_type=operation_type,
                    excluded_ips=excluded_ips,
                    task_id=task_id,
                )
            )
            is not None
        ]
    else:
        reduced_s3_log_lines = [
            reduced_s3_log_line
            for raw_s3_log_lines_buffer in progress_bar_iterator
            for raw_s3_log_line in raw_s3_log_lines_buffer
            if (reduced_s3_log_line := _reduce_raw_s3_log_line(raw_s3_log_line=raw_s3_log_line, task_id=task_id))
            is not None
        ]

    with open(file=reduced_s3_log_file_path, mode="w") as io:
        io.writelines(reduced_s3_log_lines)


def _fast_dandi_reduce_raw_s3_log_line(
    *,
    raw_s3_log_line: str,
    operation_type: str,  # Should be the literal of types, but simplifying for speed here
    excluded_ips: collections.defaultdict[str, bool],
    task_id: str,
) -> str | None:
    """
    A faster version of the parsing that makes restrictive but relatively safe assumptions about the line format.

    We trust here that various fields will exist at precise and regular positions in the string split by spaces.
    """
    try:
        split_by_space = raw_s3_log_line.split(" ")

        ip_address = split_by_space[4]
        if excluded_ips[ip_address] is True:
            return None

        line_operation_type = split_by_space[7]
        if line_operation_type != operation_type:
            return None

        full_object_key = split_by_space[8]
        full_object_key_split_by_slash = full_object_key.split("/")
        object_key_parent = full_object_key_split_by_slash[0]
        match object_key_parent:
            case "blobs":
                object_key = full_object_key
            case "zarr":
                object_key = "/".join(full_object_key_split_by_slash[:2])
            case _:
                return None

        first_post_quote_block = raw_s3_log_line.split('" ')[1].split(" ")
        http_status_code = first_post_quote_block[0]
        bytes_sent = first_post_quote_block[2]
        if len(first_post_quote_block) != 7 or not http_status_code.isdigit() or not bytes_sent.isdigit():
            return _reduce_raw_s3_log_line(raw_s3_log_line=raw_s3_log_line, task_id=task_id)
        elif http_status_code[0] != "2":
            return None

        # Forget about timezone for fast case
        timestamp = datetime.datetime.strptime("".join(split_by_space[2:3]), "[%d/%b/%Y:%H:%M:%S").isoformat()

        reduced_s3_log_line = f"{timestamp}\t{ip_address}\t{object_key}\t{bytes_sent}\n"

        return reduced_s3_log_line
    except Exception:
        message = f"Error during fast reduction of line '{raw_s3_log_line}'"
        _collect_error(message=message, error_type="fast_line_reduction", task_id=task_id)


def _reduce_raw_s3_log_line(
    *,
    raw_s3_log_line: str,
    operation_type: str,
    excluded_ips: collections.defaultdict[str, bool],
    object_key_handler: Callable,
    task_id: str,
) -> str | None:
    try:
        parsed_s3_log_line = _parse_s3_log_line(raw_s3_log_line=raw_s3_log_line)
        full_log_line = _get_full_log_line(parsed_s3_log_line=parsed_s3_log_line)
    except Exception as exception:
        message = (
            f"Error parsing line: {raw_s3_log_line}\n\n"
            f"{type(exception)}: str{exception}\n\n"
            f"{traceback.format_exc()}",
        )
        _collect_error(message=message, error_type="line_reduction", task_id=task_id)

        return None

    # Deviant log entry; usually some very ill-formed content in the URI
    # Dump information to a log file in the base folder for easy sharing
    if full_log_line is None:
        message = f"Error during parsing of line '{raw_s3_log_line}'"
        _collect_error(message=message, error_type="line")
        return None

    # Apply some minimal validation and contribute any invalidations to error collection
    # These might slow parsing down a bit, but could be important to ensuring accuracy
    if not full_log_line.status_code.isdigit():
        message = f"Unexpected status code: '{full_log_line.status_code}' parsed from line '{raw_s3_log_line}'."
        _collect_error(message=message, error_type="line", task_id=task_id)

        return None

    if _IS_OPERATION_TYPE_KNOWN[full_log_line.operation] is False:
        message = f"Unexpected request type: '{full_log_line.operation}' parsed from line '{raw_s3_log_line}'."
        _collect_error(message=message, error_type="line", task_id=task_id)

        return None

    timezone = full_log_line.timestamp[-5:]
    is_timezone_utc = timezone != "+0000"
    if is_timezone_utc:
        message = f"Unexpected time shift parsed from line '{raw_s3_log_line}'."
        _collect_error(message=message, error_type="line", task_id=task_id)
        # Fine to proceed; just wanted to be made aware if there is ever a difference so can try to investigate why

    # More early skip conditions after validation
    # Only accept 200-block status codes
    if full_log_line.status_code[0] != "2":
        return None

    if full_log_line.operation != operation_type:
        return None

    if excluded_ips[full_log_line.ip_address] is True:
        return None

    # All early skip conditions done; the line is parsed so bin the reduced information by handled asset ID
    handled_object_key = object_key_handler(raw_asset_id=full_log_line.asset_id)
    handled_timestamp = datetime.datetime.strptime(full_log_line.timestamp[:-6], "%d/%b/%Y:%H:%M:%S").isoformat()
    handled_bytes_sent = int(full_log_line.bytes_sent) if full_log_line.bytes_sent != "-" else 0

    # TODO: generalize this
    reduced_s3_log_line = (
        f"{handled_timestamp}\t{full_log_line.ip_address}\t{handled_object_key}\t{handled_bytes_sent}\n"
    )

    return reduced_s3_log_line
