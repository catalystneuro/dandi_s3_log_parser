"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import pathlib
from collections.abc import Callable
from typing import Literal

import pandas
import tqdm

from ._buffered_text_reader import BufferedTextReader
from ._s3_log_line_parser import _append_reduced_log_line


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
) -> None:
    """
    Parse a raw S3 log file and write the results to a folder of TSV files, one for each unique asset ID.

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
    """
    raw_s3_log_file_path = pathlib.Path(raw_s3_log_file_path)
    parsed_s3_log_folder_path = pathlib.Path(parsed_s3_log_folder_path)
    parsed_s3_log_folder_path.mkdir(exist_ok=True)
    bucket = bucket or ""
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    asset_id_handler = asset_id_handler or (lambda asset_id: asset_id)
    tqdm_kwargs = tqdm_kwargs or dict()

    assert raw_s3_log_file_path.suffix == ".log", f"`{raw_s3_log_file_path=}` should end in '.log'!"

    reduced_and_binned_logs = _get_reduced_and_binned_log_lines(
        raw_s3_log_file_path=raw_s3_log_file_path,
        asset_id_handler=asset_id_handler,
        bucket=bucket,
        request_type=request_type,
        excluded_ips=excluded_ips,
        tqdm_kwargs=tqdm_kwargs,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )

    for handled_asset_id, reduced_logs_per_handled_asset_id in reduced_and_binned_logs.items():
        parsed_s3_log_file_path = parsed_s3_log_folder_path / f"{handled_asset_id}.tsv"

        data_frame = pandas.DataFrame(data=reduced_logs_per_handled_asset_id)

        header = False if parsed_s3_log_file_path.exists() is True and mode == "a" else True
        data_frame.to_csv(path_or_buf=parsed_s3_log_file_path, mode=mode, sep="\t", header=header, index=False)


def _get_reduced_and_binned_log_lines(
    *,
    raw_s3_log_file_path: pathlib.Path,
    asset_id_handler: Callable,
    bucket: str,
    request_type: Literal["GET", "PUT"],
    excluded_ips: collections.defaultdict[str, bool],
    tqdm_kwargs: dict,
    maximum_buffer_size_in_bytes: int,
) -> collections.defaultdict[str, dict[str, list[str | int]]]:
    """
    Reduce the full S3 log file to minimal content and bin by asset ID.

    Parameters
    ----------
    raw_s3_log_file_path : str or pathlib.Path
        Path to the raw S3 log file.
    asset_id_handler : callable, optional
        If your asset IDs in the raw log require custom handling (i.e., they contain slashes that you do not wish to
        translate into nested directory paths) then define a function of the following form:

        # For example
        def asset_id_handler(*, raw_asset_id: str) -> str:
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]
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

    Returns
    -------
    reduced_and_binned_logs : collections.defaultdict
        A map of all reduced log line content binned by handled asset ID.
    """
    tqdm_kwargs = tqdm_kwargs or dict()

    # Perform I/O read in batches to improve performance
    resolved_tqdm_kwargs = dict(desc="Parsing line buffers...", leave=False, mininterval=5.0)
    resolved_tqdm_kwargs.update(tqdm_kwargs)

    reduced_and_binned_logs = collections.defaultdict(list)
    buffered_text_reader = BufferedTextReader(
        file_path=raw_s3_log_file_path,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )
    progress_bar_iterator = tqdm.tqdm(
        iterable=buffered_text_reader,
        total=len(buffered_text_reader),
        **resolved_tqdm_kwargs,
    )

    per_buffer_index = 0
    for buffered_raw_lines in progress_bar_iterator:
        for index, raw_line in enumerate(buffered_raw_lines):
            line_index = per_buffer_index + index

            _append_reduced_log_line(
                raw_line=raw_line,
                reduced_and_binned_logs=reduced_and_binned_logs,
                asset_id_handler=asset_id_handler,
                bucket=bucket,
                request_type=request_type,
                excluded_ips=excluded_ips,
                log_file_path=raw_s3_log_file_path,
                line_index=line_index,
            )
        per_buffer_index += index

    return reduced_and_binned_logs
