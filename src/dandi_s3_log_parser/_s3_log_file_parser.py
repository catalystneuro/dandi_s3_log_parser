"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import datetime
import pathlib
import os
import shutil
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, Literal

import pandas
import tqdm
import natsort

from ._ip_utils import (
    _get_latest_github_ip_ranges,
    _load_ip_address_to_region_cache,
    _save_ip_address_to_region_cache,
)
from ._s3_log_line_parser import ReducedLogLine, _append_reduced_log_line
from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH
from ._buffered_text_reader import BufferedTextReader
from ._order_parsed_logs import order_parsed_logs


def parse_all_dandi_raw_s3_logs(
    *,
    base_raw_s3_log_folder_path: str | pathlib.Path,
    parsed_s3_log_folder_path: str | pathlib.Path,
    mode: Literal["w", "a"] = "a",
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    exclude_github_ips: bool = True,
    number_of_jobs: int = 1,
    maximum_ram_usage_in_bytes: int = 4 * 10**9,
) -> None:
    """
    Batch parse all raw S3 log files in a folder and write the results to a folder of TSV files.

    Assumes the following folder structure...

    |- <base_raw_s3_log_folder_path>
    |-- 2019 (year)
    |--- 01 (month)
    |---- 01.log (day)
    | ...

    Parameters
    ----------
    base_raw_s3_log_folder_path : string or pathlib.Path
        Path to the folder containing the raw S3 log files.
    parsed_s3_log_folder_path : string or pathlib.Path
        Path to write each parsed S3 log file to.
        There will be one file per handled asset ID.
    mode : "w" or "a", default: "a"
        How to resolve the case when files already exist in the folder containing parsed logs.
        "w" will overwrite existing content, "a" will append or create if the file does not yet exist.
    excluded_ips : collections.defaultdict of strings to booleans, optional
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    exclude_github_ips : bool, default: True
        Include all GitHub action IP addresses in the `excluded_ips`.
    number_of_jobs : int, default: 1
        The number of jobs to use for parallel processing.
        Allows negative range to mean 'all but this many (minus one) jobs'.
        E.g., -1 means use all workers, -2 means all but one worker.
        WARNING: planned but not yet supported.
    maximum_ram_usage_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to be used across all the processes.
    """
    base_raw_s3_log_folder_path = pathlib.Path(base_raw_s3_log_folder_path)
    parsed_s3_log_folder_path = pathlib.Path(parsed_s3_log_folder_path)
    parsed_s3_log_folder_path.mkdir(exist_ok=True)

    # Re-define some top-level pass-through items here to avoid repeated constructions
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    if exclude_github_ips:
        for github_ip in _get_latest_github_ip_ranges():
            excluded_ips[github_ip] = True

    def asset_id_handler(*, raw_asset_id: str) -> str:
        split_by_slash = raw_asset_id.split("/")
        return split_by_slash[0] + "_" + split_by_slash[-1]

    daily_raw_s3_log_file_paths = list()
    base_folder_paths = [path for path in base_raw_s3_log_folder_path.iterdir() if path.stem.startswith("20")]
    yearly_folder_paths = natsort.natsorted(seq=list(base_folder_paths))
    for yearly_folder_path in yearly_folder_paths:
        monthly_folder_paths = natsort.natsorted(seq=list(yearly_folder_path.iterdir()))

        for monthly_folder_path in monthly_folder_paths:
            daily_raw_s3_log_file_paths.extend(natsort.natsorted(seq=list(monthly_folder_path.glob("*.log"))))

    if number_of_jobs == 1:
        for raw_s3_log_file_path in tqdm.tqdm(
            iterable=daily_raw_s3_log_file_paths,
            desc="Parsing log files...",
            position=0,
            leave=True,
        ):
            parse_dandi_raw_s3_log(
                raw_s3_log_file_path=raw_s3_log_file_path,
                parsed_s3_log_folder_path=parsed_s3_log_folder_path,
                mode=mode,
                excluded_ips=excluded_ips,
                exclude_github_ips=False,  # Already included in list so avoid repeated construction
                asset_id_handler=asset_id_handler,
                tqdm_kwargs=dict(position=1, leave=False),
                maximum_ram_usage_in_bytes=maximum_ram_usage_in_bytes,
            )
    else:
        # Create a fresh temporary directory in the home folder and then fresh subfolders for each job
        temporary_base_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "temp"
        temporary_base_folder_path.mkdir(exist_ok=True)

        # Clean up any previous tasks that failed to clean themselves up
        for previous_task_folder_path in temporary_base_folder_path.iterdir():
            shutil.rmtree(path=previous_task_folder_path, ignore_errors=True)

        task_id = uuid.uuid4()[:5]
        temporary_folder_path = temporary_base_folder_path / task_id
        temporary_folder_path.mkdir(exist_ok=True)
        per_job_temporary_folder_paths = list()
        for job_index in range(number_of_jobs):
            per_job_temporary_folder_path = temporary_folder_path / f"job_{job_index}"
            per_job_temporary_folder_path.mkdir(exist_ok=True)
            per_job_temporary_folder_paths.append(per_job_temporary_folder_path)

        maximum_ram_usage_in_bytes_per_job = maximum_ram_usage_in_bytes // number_of_jobs

        futures = []
        with ProcessPoolExecutor(max_workers=number_of_jobs) as executor:
            for raw_s3_log_file_path in daily_raw_s3_log_file_paths:
                futures.append(
                    executor.submit(
                        _multi_job_parse_dandi_raw_s3_log,
                        number_of_jobs=number_of_jobs,
                        raw_s3_log_file_path=raw_s3_log_file_path,
                        temporary_folder_path=temporary_folder_path,
                        mode=mode,
                        excluded_ips=excluded_ips,
                        exclude_github_ips=False,  # Already included in list so avoid repeated construction
                        asset_id_handler=asset_id_handler,
                        maximum_ram_usage_in_bytes=maximum_ram_usage_in_bytes_per_job,
                    )
                )

            # Perform the iteration to trigger processing
            for _ in tqdm.tqdm(
                iterable=as_completed(daily_raw_s3_log_file_paths),
                desc=f"Parsing log files using {number_of_jobs} jobs...",
                total=len(daily_raw_s3_log_file_paths),
                position=0,
                leave=True,
            ):
                pass

        merged_temporary_folder_path = temporary_folder_path / "merged"
        merged_temporary_folder_path.mkdir(exist_ok=True)

        print("\n\nParallel parsing complete!\n\n")

        for per_job_temporary_folder_path in tqdm.tqdm(
            iterable=per_job_temporary_folder_paths,
            desc="Merging results across jobs...",
            total=len(per_job_temporary_folder_paths),
            position=0,
            leave=True,
        ):
            per_job_parsed_s3_log_file_paths = list(per_job_temporary_folder_path.iterdir())
            for per_job_parsed_s3_log_file_path in tqdm.tqdm(
                iterable=per_job_parsed_s3_log_file_paths,
                desc="Merging results per job...",
                total=len(per_job_parsed_s3_log_file_paths),
                position=1,
                leave=False,
                mininterval=1.0,
            ):
                merged_temporary_file_path = merged_temporary_folder_path / per_job_parsed_s3_log_file_path.name

                parsed_s3_log = pandas.read_table(filepath_or_buffer=per_job_parsed_s3_log_file_path, index_col=0)
                parsed_s3_log.to_csv(path_or_buf=merged_temporary_file_path, mode="a", sep="\t")

        order_parsed_logs(
            unordered_parsed_s3_log_folder_path=merged_temporary_folder_path,
            ordered_parsed_s3_log_folder_path=parsed_s3_log_folder_path,
        )

        shutil.rmtree(path=temporary_folder_path, ignore_errors=True)

    return None


def _multi_job_parse_dandi_raw_s3_log(
    *,
    number_of_jobs: int,
    raw_s3_log_file_path: pathlib.Path,
    temporary_folder_path: pathlib.Path,
    mode: Literal["w", "a"],
    excluded_ips: collections.defaultdict[str, bool] | None,
    exclude_github_ips: bool,
    asset_id_handler: Callable | None,
    maximum_ram_usage_in_bytes: int,
) -> None:
    """A mostly pass-through function to calculate the job index on the worker and target the correct subfolder."""
    job_index = os.getpid() % number_of_jobs
    per_job_temporary_folder_path = temporary_folder_path / f"job_{job_index}"

    parse_dandi_raw_s3_log(
        raw_s3_log_file_path=raw_s3_log_file_path,
        parsed_s3_log_folder_path=per_job_temporary_folder_path,
        mode=mode,
        excluded_ips=excluded_ips,
        exclude_github_ips=exclude_github_ips,
        asset_id_handler=asset_id_handler,
        tqdm_kwargs=dict(position=job_index + 1, leave=False),
        maximum_ram_usage_in_bytes=maximum_ram_usage_in_bytes,
    )

    return None


def parse_dandi_raw_s3_log(
    *,
    raw_s3_log_file_path: str | pathlib.Path,
    parsed_s3_log_folder_path: str | pathlib.Path,
    mode: Literal["w", "a"] = "a",
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    exclude_github_ips: bool = True,
    asset_id_handler: Callable | None = None,
    tqdm_kwargs: dict | None = None,
    maximum_ram_usage_in_bytes: int = 4 * 10**9,
) -> None:
    """
    Parse a raw S3 log file and write the results to a folder of TSV files, one for each unique asset ID.

    'Parsing' here means:
      - limiting only to requests of the specified type (i.e., GET, PUT, etc.)
      - reducing the information to the asset ID, request time, request size, and geographic IP of the requester

    Parameters
    ----------
    raw_s3_log_file_path : string or pathlib.Path
        Path to the raw S3 log file.
    parsed_s3_log_folder_path : string or pathlib.Path
        The path to write each parsed S3 log file to.
        There will be one file per handled asset ID.
    mode : "w" or "a", default: "a"
        How to resolve the case when files already exist in the folder containing parsed logs.
        "w" will overwrite existing content, "a" will append or create if the file does not yet exist.

        The intention of the default usage is to have one consolidated raw S3 log file per day and then to iterate
        over each day, parsing and binning by asset, effectively 'updating' the parsed collection on each iteration.
        HINT: If this iteration is done in chronological order, the resulting parsed logs will also maintain that order.
    excluded_ips : collections.defaultdict of strings to booleans, optional
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    exclude_github_ips : bool, default: True
        Include all GitHub action IP addresses in the `excluded_ips`.
    asset_id_handler : callable, optional
        If your asset IDs in the raw log require custom handling (i.e., they contain slashes that you do not wish to
        translate into nested directory paths) then define a function of the following form:

        # For example
        def asset_id_handler(*, raw_asset_id: str) -> str:
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]
    tqdm_kwargs : dict, optional
        Keyword arguments to pass to the tqdm progress bar.
    maximum_ram_usage_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to be used throughout the process.
    """
    tqdm_kwargs = tqdm_kwargs or dict()

    bucket = "dandiarchive"
    request_type = "GET"

    # Form a lookup for IP addresses to exclude; much faster than asking 'if in' a list on each iteration
    # Exclude GitHub actions, which are responsible for running health checks on archive which bloat the logs
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    if exclude_github_ips:
        for github_ip in _get_latest_github_ip_ranges():
            excluded_ips[github_ip] = True

    if asset_id_handler is None:

        def asset_id_handler(*, raw_asset_id: str) -> str:
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]

    return parse_raw_s3_log(
        raw_s3_log_file_path=raw_s3_log_file_path,
        parsed_s3_log_folder_path=parsed_s3_log_folder_path,
        mode=mode,
        bucket=bucket,
        request_type=request_type,
        excluded_ips=excluded_ips,
        asset_id_handler=asset_id_handler,
        tqdm_kwargs=tqdm_kwargs,
        maximum_ram_usage_in_bytes=maximum_ram_usage_in_bytes,
    )


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
    maximum_ram_usage_in_bytes: int = 4 * 10**9,
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
    maximum_ram_usage_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to be used throughout the process.
    """
    raw_s3_log_file_path = pathlib.Path(raw_s3_log_file_path)
    parsed_s3_log_folder_path = pathlib.Path(parsed_s3_log_folder_path)
    parsed_s3_log_folder_path.mkdir(exist_ok=True)
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    tqdm_kwargs = tqdm_kwargs or dict()

    reduced_logs = _get_reduced_log_lines(
        raw_s3_log_file_path=raw_s3_log_file_path,
        bucket=bucket,
        request_type=request_type,
        excluded_ips=excluded_ips,
        tqdm_kwargs=tqdm_kwargs,
        maximum_ram_usage_in_bytes=maximum_ram_usage_in_bytes,
    )

    reduced_logs_binned_by_unparsed_asset = dict()
    for reduced_log in reduced_logs:
        raw_asset_id = reduced_log.asset_id
        reduced_logs_binned_by_unparsed_asset[raw_asset_id] = reduced_logs_binned_by_unparsed_asset.get(
            raw_asset_id, collections.defaultdict(list)
        )

        reduced_logs_binned_by_unparsed_asset[raw_asset_id]["timestamp"].append(reduced_log.timestamp)
        reduced_logs_binned_by_unparsed_asset[raw_asset_id]["bytes_sent"].append(reduced_log.bytes_sent)
        reduced_logs_binned_by_unparsed_asset[raw_asset_id]["region"].append(reduced_log.region)

    if asset_id_handler is not None:
        reduced_logs_binned_by_asset = dict()
        for raw_asset_id, reduced_logs_per_asset in reduced_logs_binned_by_unparsed_asset.items():
            parsed_asset_id = asset_id_handler(raw_asset_id=raw_asset_id)

            reduced_logs_binned_by_asset[parsed_asset_id] = reduced_logs_per_asset
    else:
        reduced_logs_binned_by_asset = reduced_logs_binned_by_unparsed_asset

    for raw_asset_id, reduced_logs_per_asset in reduced_logs_binned_by_asset.items():
        parsed_s3_log_file_path = parsed_s3_log_folder_path / f"{raw_asset_id}.tsv"

        data_frame = pandas.DataFrame(data=reduced_logs_per_asset)
        data_frame.to_csv(path_or_buf=parsed_s3_log_file_path, mode=mode, sep="\t")

    progress_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "progress"
    progress_folder_path.mkdir(exist_ok=True)

    date = datetime.datetime.now().strftime("%y%m%d")
    progress_file_path = progress_folder_path / f"{date}.txt"
    with open(file=progress_file_path, mode="a") as io:
        io.write(f"Parsed {raw_s3_log_file_path} successfully!\n")

    return None


def _get_reduced_log_lines(
    *,
    raw_s3_log_file_path: pathlib.Path,
    bucket: str | None,
    request_type: Literal["GET", "PUT"],
    excluded_ips: collections.defaultdict[str, bool],
    tqdm_kwargs: dict | None = None,
    maximum_ram_usage_in_bytes: int = 4 * 10**9,
) -> list[ReducedLogLine]:
    """
    Reduce the full S3 log file to minimal content and return a list of in-memory collections.namedtuple objects.

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
    maximum_ram_usage_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to be used throughout the process.
    """
    assert raw_s3_log_file_path.suffix == ".log", f"{raw_s3_log_file_path=} should end in '.log'!"

    # Collapse bucket to empty string instead of asking if it is None on each iteration
    bucket = "" if bucket is None else bucket
    tqdm_kwargs = tqdm_kwargs or dict()

    # One-time initialization/read of IP address to region cache for performance
    # This dictionary is intended to be mutated throughout the process
    ip_address_to_region = _load_ip_address_to_region_cache()

    # Perform I/O read in batches to improve performance
    resolved_tqdm_kwargs = dict(desc="Parsing line buffers...", leave=False, mininterval=1.0)
    resolved_tqdm_kwargs.update(tqdm_kwargs)

    reduced_log_lines = list()
    per_buffer_index = 0
    buffered_text_reader = BufferedTextReader(
        file_path=raw_s3_log_file_path, maximum_ram_usage_in_bytes=maximum_ram_usage_in_bytes
    )
    for buffered_raw_lines in tqdm.tqdm(iterable=buffered_text_reader, **resolved_tqdm_kwargs):
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
                ip_hash_to_region=ip_address_to_region,
            )
            index += 1
        per_buffer_index += index

    _save_ip_address_to_region_cache(ip_hash_to_region=ip_address_to_region)

    return reduced_log_lines
