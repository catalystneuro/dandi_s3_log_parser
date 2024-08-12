"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import pathlib
import shutil
import uuid
from collections.abc import Callable
from typing import Literal

import pandas
import tqdm

from ._buffered_text_reader import BufferedTextReader
from ._order_and_anonymize_parsed_logs import order_and_anonymize_parsed_logs
from ._s3_log_line_parser import _append_reduced_log_line, _ReducedLogLine


def parse_raw_s3_log(
    *,
    raw_s3_log_file_path: str | pathlib.Path,
    parsed_s3_log_folder_path: str | pathlib.Path,
    mode: Literal["w", "a"] = "a",
    bucket: str | None = None,
    request_type: Literal["GET", "PUT"] = "GET",
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    asset_id_handler: Callable | None = None,
    tqdm_kwargs: dict | None = None,
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    order_results: bool = True,
) -> None:
    """Parse a raw S3 log file and write the results to a folder of TSV files, one for each unique asset ID.

    'Parsing' here means:
      - limiting only to requests of the specified type (i.e., GET, PUT, etc.)
      - reducing the information to the asset ID, request time, request size, and geographic IP of the requester

    Parameters
    ----------
    raw_s3_log_file_path : str or pathlib.Path
        Path to the raw S3 log file.
    parsed_s3_log_folder_path : str or pathlib.Path
        Path to write each parsed S3 log file to.
        There will be one file per handled asset ID.
    mode : "w" or "a", default: "a"
        How to resolve the case when files already exist in the folder containing parsed logs.
        "w" will overwrite existing content, "a" will append or create if the file does not yet exist.

        The intention of the default usage is to have one consolidated raw S3 log file per day and then to iterate
        over each day, parsing and binning by asset, effectively 'updating' the parsed collection on each iteration.
        HINT: If this iteration is done in chronological order, the resulting parsed logs will also maintain that order.
    bucket : str
        Only parse and return lines that match this bucket.
    request_type : str, default: "GET"
        The type of request to filter for.
    excluded_ips : collections.defaultdict of strings to booleans, optional
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    asset_id_handler : callable, optional
        If your asset IDs in the raw log require custom handling (i.e., they contain slashes that you do not wish to
        translate into nested directory paths) then define a function of the following form:

        # For example
        def asset_id_handler(*, raw_asset_id: str) -> str:
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]
    tqdm_kwargs : dict, optional
        Keyword arguments to pass to the tqdm progress bar.
    maximum_buffer_size_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to use on each buffer iteration when reading from the
        source text file.
    order_results : bool, default: True
        Whether to order the results chronologically.
        This is strongly suggested, but a common case of disabling it is if ordering is intended to be applied after
        multiple steps of processing instead of during this operation.

    """
    raw_s3_log_file_path = pathlib.Path(raw_s3_log_file_path)
    parsed_s3_log_folder_path = pathlib.Path(parsed_s3_log_folder_path)
    parsed_s3_log_folder_path.mkdir(exist_ok=True)
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    tqdm_kwargs = tqdm_kwargs or dict()

    if order_results is True:
        # Create a fresh temporary directory in the home folder and then fresh subfolders for each job
        temporary_base_folder_path = parsed_s3_log_folder_path / ".temp"
        shutil.rmtree(path=temporary_base_folder_path, ignore_errors=True)
        temporary_base_folder_path.mkdir(exist_ok=True)

        task_id = str(uuid.uuid4())[:5]
        temporary_folder_path = temporary_base_folder_path / task_id
        temporary_folder_path.mkdir(exist_ok=True)
        temporary_output_folder_path = temporary_folder_path / "output"
        temporary_output_folder_path.mkdir(exist_ok=True)

    reduced_logs = _get_reduced_log_lines(
        raw_s3_log_file_path=raw_s3_log_file_path,
        bucket=bucket,
        request_type=request_type,
        excluded_ips=excluded_ips,
        tqdm_kwargs=tqdm_kwargs,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )

    reduced_logs_binned_by_unparsed_asset = dict()
    for reduced_log in reduced_logs:
        raw_asset_id = reduced_log.asset_id
        reduced_logs_binned_by_unparsed_asset[raw_asset_id] = reduced_logs_binned_by_unparsed_asset.get(
            raw_asset_id, collections.defaultdict(list),
        )

        reduced_logs_binned_by_unparsed_asset[raw_asset_id]["timestamp"].append(reduced_log.timestamp)
        reduced_logs_binned_by_unparsed_asset[raw_asset_id]["bytes_sent"].append(reduced_log.bytes_sent)
        reduced_logs_binned_by_unparsed_asset[raw_asset_id]["ip_address"].append(reduced_log.ip_address)

    if asset_id_handler is not None:
        reduced_logs_binned_by_asset = dict()
        for raw_asset_id, reduced_logs_per_asset in reduced_logs_binned_by_unparsed_asset.items():
            parsed_asset_id = asset_id_handler(raw_asset_id=raw_asset_id)

            reduced_logs_binned_by_asset[parsed_asset_id] = reduced_logs_per_asset
    else:
        reduced_logs_binned_by_asset = reduced_logs_binned_by_unparsed_asset

    for raw_asset_id, reduced_logs_per_asset in reduced_logs_binned_by_asset.items():
        output_folder_path = temporary_output_folder_path if order_results is True else parsed_s3_log_folder_path
        parsed_s3_log_file_path = output_folder_path / f"{raw_asset_id}.tsv"

        data_frame = pandas.DataFrame(data=reduced_logs_per_asset)

        header = False if parsed_s3_log_file_path.exists() is True and mode == "a" else True
        data_frame.to_csv(path_or_buf=parsed_s3_log_file_path, mode=mode, sep="\t", header=header, index=False)

    if order_results is True:
        order_and_anonymize_parsed_logs(
            unordered_parsed_s3_log_folder_path=temporary_output_folder_path,
            anonymized_s3_log_folder_path=parsed_s3_log_folder_path,
        )

        shutil.rmtree(path=temporary_output_folder_path, ignore_errors=True)



def _get_reduced_log_lines(
    *,
    raw_s3_log_file_path: pathlib.Path,
    bucket: str | None,
    request_type: Literal["GET", "PUT"],
    excluded_ips: collections.defaultdict[str, bool],
    tqdm_kwargs: dict | None = None,
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    ip_hash_to_region_file_path: pathlib.Path | None,
) -> list[_ReducedLogLine]:
    """Reduce the full S3 log file to minimal content and return a list of in-memory collections.namedtuple objects.

    Parameters
    ----------
    raw_s3_log_file_path : str or pathlib.Path
        Path to the raw S3 log file.
    bucket : str
        Only parse and return lines that match this bucket.
    request_type : str
        The type of request to filter for.
    excluded_ips : collections.defaultdict of strings to booleans
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    tqdm_kwargs : dict, optional
        Keyword arguments to pass to the tqdm progress bar.
    maximum_buffer_size_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to use on each buffer iteration when reading from the
        source text file.

    """
    assert raw_s3_log_file_path.suffix == ".log", f"{raw_s3_log_file_path=} should end in '.log'!"

    # Collapse bucket to empty string instead of asking if it is None on each iteration
    bucket = "" if bucket is None else bucket
    tqdm_kwargs = tqdm_kwargs or dict()

    # Perform I/O read in batches to improve performance
    resolved_tqdm_kwargs = dict(desc="Parsing line buffers...", leave=False, mininterval=1.0)
    resolved_tqdm_kwargs.update(tqdm_kwargs)

    reduced_log_lines = list()
    per_buffer_index = 0
    buffered_text_reader = BufferedTextReader(
        file_path=raw_s3_log_file_path, maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )
    for buffered_raw_lines in tqdm.tqdm(
        iterable=buffered_text_reader, total=len(buffered_text_reader), **resolved_tqdm_kwargs,
    ):
        index = 0
        for raw_line in buffered_raw_lines:
            _append_reduced_log_line(
                raw_line=raw_line,
                reduced_log_lines=reduced_log_lines,
                bucket=bucket,
                request_type=request_type,
                excluded_ips=excluded_ips,
                log_file_path=raw_s3_log_file_path,
                index=index,
            )
            index += 1
        per_buffer_index += index

    return reduced_log_lines
