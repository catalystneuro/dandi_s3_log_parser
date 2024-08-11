"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import datetime
import pathlib
import os
import shutil
import traceback
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable, Literal
import importlib.metadata

import pandas
from pydantic import validate_call, Field
import tqdm

from ._ip_utils import (
    _get_latest_github_ip_ranges,
)
from ._s3_log_file_parser import parse_raw_s3_log
from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH
from ._order_parsed_logs import order_parsed_logs
from ._ip_utils import _load_ip_address_to_region_cache, _save_ip_address_to_region_cache, _IP_HASH_TO_REGION_FILE_PATH


@validate_call
def parse_all_dandi_raw_s3_logs(
    *,
    base_raw_s3_log_folder_path: str | pathlib.Path,
    parsed_s3_log_folder_path: str | pathlib.Path,
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    exclude_github_ips: bool = True,
    maximum_number_of_workers: int = Field(ge=1, le=os.cpu_count(), default=1),
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
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
    excluded_ips : collections.defaultdict of strings to booleans, optional
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
    exclude_github_ips : bool, default: True
        Include all GitHub action IP addresses in the `excluded_ips`.
    maximum_number_of_workers : int, default: 1
        The maximum number of workers to distribute tasks across.
    maximum_buffer_size_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to use on each buffer iteration when reading from the
        source text files.
        Actual total RAM usage will be higher due to overhead and caching.
        Automatically splits this total amount over the maximum number of workers if `maximum_number_of_workers` is
        greater than one.
    """
    base_raw_s3_log_folder_path = pathlib.Path(base_raw_s3_log_folder_path)
    parsed_s3_log_folder_path = pathlib.Path(parsed_s3_log_folder_path)
    parsed_s3_log_folder_path.mkdir(exist_ok=True)

    # Create a fresh temporary directory in the home folder and then fresh subfolders for each job
    temporary_base_folder_path = parsed_s3_log_folder_path / ".temp"
    temporary_base_folder_path.mkdir(exist_ok=True)

    # Clean up any previous tasks that failed to clean themselves up
    for previous_task_folder_path in temporary_base_folder_path.iterdir():
        shutil.rmtree(path=previous_task_folder_path, ignore_errors=True)

    task_id = str(uuid.uuid4())[:5]
    temporary_folder_path = temporary_base_folder_path / task_id
    temporary_folder_path.mkdir(exist_ok=True)

    temporary_output_folder_path = temporary_folder_path / "output"
    temporary_output_folder_path.mkdir(exist_ok=True)

    # Re-define some top-level pass-through items here to avoid repeated constructions
    excluded_ips = excluded_ips or collections.defaultdict(bool)
    if exclude_github_ips:
        for github_ip in _get_latest_github_ip_ranges():
            excluded_ips[github_ip] = True

    def asset_id_handler(*, raw_asset_id: str) -> str:
        split_by_slash = raw_asset_id.split("/")
        return split_by_slash[0] + "_" + split_by_slash[-1]

    daily_raw_s3_log_file_paths = set(base_raw_s3_log_folder_path.rglob(pattern="*.log"))

    # Workaround to particular issue with current repo storage structure on Drogon
    daily_raw_s3_log_file_paths.remove(pathlib.Path("/mnt/backup/dandi/dandiarchive-logs/stats/start-end.log"))

    daily_raw_s3_log_file_paths = list(daily_raw_s3_log_file_paths)[:30]

    if maximum_number_of_workers == 1:
        for raw_s3_log_file_path in tqdm.tqdm(
            iterable=daily_raw_s3_log_file_paths,
            desc="Parsing log files...",
            position=0,
            leave=True,
        ):
            parse_dandi_raw_s3_log(
                raw_s3_log_file_path=raw_s3_log_file_path,
                parsed_s3_log_folder_path=temporary_output_folder_path,
                mode="a",
                excluded_ips=excluded_ips,
                exclude_github_ips=False,  # Already included in list so avoid repeated construction
                asset_id_handler=asset_id_handler,
                tqdm_kwargs=dict(position=1, leave=False),
                maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
                order_results=False,  # Will immediately reorder all files at the end
            )
    else:
        ip_hash_to_region = _load_ip_address_to_region_cache()

        per_job_temporary_folder_paths = list()
        for job_index in range(maximum_number_of_workers):
            per_job_temporary_folder_path = temporary_folder_path / f"job_{job_index}"
            per_job_temporary_folder_path.mkdir(exist_ok=True)
            per_job_temporary_folder_paths.append(per_job_temporary_folder_path)

            # Must have one cache copy per job to avoid race conditions
            ip_hash_to_region_file_path = per_job_temporary_folder_path / "ip_hash_to_region.yaml"
            _save_ip_address_to_region_cache(
                ip_hash_to_region=ip_hash_to_region, ip_hash_to_region_file_path=ip_hash_to_region_file_path
            )

        maximum_buffer_size_in_bytes_per_job = maximum_buffer_size_in_bytes // maximum_number_of_workers

        futures = []
        with ProcessPoolExecutor(max_workers=maximum_number_of_workers) as executor:
            for raw_s3_log_file_path in daily_raw_s3_log_file_paths:
                futures.append(
                    executor.submit(
                        _multi_job_parse_dandi_raw_s3_log,
                        maximum_number_of_workers=maximum_number_of_workers,
                        raw_s3_log_file_path=raw_s3_log_file_path,
                        temporary_folder_path=temporary_folder_path,
                        excluded_ips=excluded_ips,
                        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes_per_job,
                    )
                )

            progress_bar_iterable = tqdm.tqdm(
                iterable=as_completed(futures),
                desc=f"Parsing log files using {maximum_number_of_workers} jobs...",
                total=len(daily_raw_s3_log_file_paths),
                position=0,
                leave=True,
            )
            for future in progress_bar_iterable:
                future.result()  # This is the call that finally triggers the deployment to the workers

        print("\n\nParallel parsing complete!\n\n")

        # Merge IP cache files
        for job_index in range(maximum_number_of_workers):
            per_job_temporary_folder_path = temporary_folder_path / f"job_{job_index}"
            ip_hash_to_region_file_path = per_job_temporary_folder_path / "ip_hash_to_region.yaml"

            new_ip_hash_to_region = _load_ip_address_to_region_cache(
                ip_hash_to_region_file_path=ip_hash_to_region_file_path
            )
            ip_hash_to_region.update(new_ip_hash_to_region)
        _save_ip_address_to_region_cache(
            ip_hash_to_region=ip_hash_to_region, ip_hash_to_region_file_path=_IP_HASH_TO_REGION_FILE_PATH
        )

        for per_job_temporary_folder_path in tqdm.tqdm(
            iterable=per_job_temporary_folder_paths,
            desc="Merging results across jobs...",
            total=len(per_job_temporary_folder_paths),
            position=0,
            leave=True,
        ):
            per_job_parsed_s3_log_file_paths = list(per_job_temporary_folder_path.iterdir())
            assert len(per_job_parsed_s3_log_file_paths) != 0, f"No files found in {per_job_temporary_folder_path}!"

            for per_job_parsed_s3_log_file_path in tqdm.tqdm(
                iterable=per_job_parsed_s3_log_file_paths,
                desc="Merging results per job...",
                total=len(per_job_parsed_s3_log_file_paths),
                position=1,
                leave=False,
                mininterval=1.0,
            ):
                merged_temporary_file_path = temporary_output_folder_path / per_job_parsed_s3_log_file_path.name

                parsed_s3_log = pandas.read_table(filepath_or_buffer=per_job_parsed_s3_log_file_path)

                header = False if merged_temporary_file_path.exists() else True
                parsed_s3_log.to_csv(
                    path_or_buf=merged_temporary_file_path, mode="a", sep="\t", header=header, index=False
                )

    # Always apply this step at the end to be sure we maintained chronological order
    # (even if you think order of iteration itself was performed chronologically)
    # This step also adds the index counter to the TSV
    order_parsed_logs(
        unordered_parsed_s3_log_folder_path=temporary_output_folder_path,
        ordered_parsed_s3_log_folder_path=parsed_s3_log_folder_path,
    )

    shutil.rmtree(path=temporary_output_folder_path, ignore_errors=True)

    return None


def _multi_job_parse_dandi_raw_s3_log(
    *,
    maximum_number_of_workers: int,
    raw_s3_log_file_path: pathlib.Path,
    temporary_folder_path: pathlib.Path,
    excluded_ips: collections.defaultdict[str, bool] | None,
    maximum_buffer_size_in_bytes: int,
) -> None:
    """
    A mostly pass-through function to calculate the job index on the worker and target the correct subfolder.

    Also dumps error stack (which is only typically seen by the worker and not sent back to the main stdout pipe)
    to a log file.
    """

    try:
        error_message = ""

        def asset_id_handler(*, raw_asset_id: str) -> str:
            """Apparently callables, even simple built-in ones, cannot be pickled."""
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]

        job_index = os.getpid() % maximum_number_of_workers
        per_job_temporary_folder_path = temporary_folder_path / f"job_{job_index}"
        ip_hash_to_region_file_path = per_job_temporary_folder_path / "ip_hash_to_region.yaml"

        # Define error catching stuff as part of the try clause
        # so that if there is a problem within that, it too is caught
        errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
        errors_folder_path.mkdir(exist_ok=True)

        dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
        date = datetime.datetime.now().strftime("%y%m%d")
        parallel_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_parallel_errors.txt"
        error_message += (
            f"Job index {job_index}/{maximum_number_of_workers} parsing {raw_s3_log_file_path} failed due to\n\n"
        )

        parse_dandi_raw_s3_log(
            raw_s3_log_file_path=raw_s3_log_file_path,
            parsed_s3_log_folder_path=per_job_temporary_folder_path,
            mode="a",
            excluded_ips=excluded_ips,
            exclude_github_ips=False,  # Already included in list so avoid repeated construction
            asset_id_handler=asset_id_handler,
            tqdm_kwargs=dict(position=job_index + 1, leave=False),
            maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
            order_results=False,  # Always disable this for parallel processing
            ip_hash_to_region_file_path=ip_hash_to_region_file_path,
        )
    except Exception as exception:
        with open(file=parallel_errors_file_path, mode="a") as io:
            error_message += f"{type(exception)}: {str(exception)}\n\n{traceback.format_exc()}\n\n"
            io.write(error_message)

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
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    order_results: bool = True,
    ip_hash_to_region_file_path: str | pathlib.Path | None = None,
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

    parse_raw_s3_log(
        raw_s3_log_file_path=raw_s3_log_file_path,
        parsed_s3_log_folder_path=parsed_s3_log_folder_path,
        mode=mode,
        bucket=bucket,
        request_type=request_type,
        excluded_ips=excluded_ips,
        asset_id_handler=asset_id_handler,
        tqdm_kwargs=tqdm_kwargs,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        order_results=order_results,
        ip_hash_to_region_file_path=ip_hash_to_region_file_path,
    )

    return None
