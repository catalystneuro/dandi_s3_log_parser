"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import datetime
import importlib.metadata
import os
import pathlib
import random
import shutil
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Literal

import pandas
import tqdm
from pydantic import DirectoryPath, Field, FilePath, validate_call

from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH
from ._s3_log_file_parser import parse_raw_s3_log


@validate_call
def parse_all_dandi_raw_s3_logs(
    *,
    base_raw_s3_log_folder_path: DirectoryPath,
    parsed_s3_log_folder_path: DirectoryPath,
    excluded_log_files: list[FilePath] | None = None,
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    maximum_number_of_workers: int = Field(ge=1, default=1),
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
    base_raw_s3_log_folder_path : file path
        Path to the folder containing the raw S3 log files.
    parsed_s3_log_folder_path : file path
        Path to write each parsed S3 log file to.
        There will be one file per handled asset ID.
    excluded_log_files : list of file paths, optional
        A list of log file paths to exclude from parsing.
    excluded_ips : collections.defaultdict of strings to booleans, optional
        A lookup table / hash map whose keys are IP addresses and values are True to exclude from parsing.
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
    excluded_log_files = excluded_log_files or list()
    excluded_log_files = {pathlib.Path(excluded_log_file) for excluded_log_file in excluded_log_files}
    excluded_ips = excluded_ips or collections.defaultdict(bool)

    asset_id_handler = _get_default_dandi_asset_id_handler()

    # The .rglob is not naturally sorted; shuffle for more uniform progress updates
    daily_raw_s3_log_file_paths = set(base_raw_s3_log_folder_path.rglob(pattern="*.log")) - excluded_log_files
    random.shuffle(list(daily_raw_s3_log_file_paths))

    if maximum_number_of_workers == 1:
        for raw_s3_log_file_path in tqdm.tqdm(
            iterable=daily_raw_s3_log_file_paths,
            desc="Parsing log files...",
            position=0,
            leave=True,
        ):
            parse_dandi_raw_s3_log(
                raw_s3_log_file_path=raw_s3_log_file_path,
                parsed_s3_log_folder_path=parsed_s3_log_folder_path,
                mode="a",
                excluded_ips=excluded_ips,
                asset_id_handler=asset_id_handler,
                tqdm_kwargs=dict(position=1, leave=False),
                maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
            )
    else:
        # Create a fresh temporary directory in the home folder and then fresh subfolders for each worker
        temporary_base_folder_path = parsed_s3_log_folder_path / ".temp"
        temporary_base_folder_path.mkdir(exist_ok=True)

        # Clean up any previous tasks that failed to clean themselves up
        for previous_task_folder_path in temporary_base_folder_path.iterdir():
            shutil.rmtree(path=previous_task_folder_path, ignore_errors=True)

        task_id = str(uuid.uuid4())[:5]
        temporary_folder_path = temporary_base_folder_path / task_id
        temporary_folder_path.mkdir(exist_ok=True)

        per_worker_temporary_folder_paths = list()
        for worker_index in range(maximum_number_of_workers):
            per_worker_temporary_folder_path = temporary_folder_path / f"worker_{worker_index}"
            per_worker_temporary_folder_path.mkdir(exist_ok=True)
            per_worker_temporary_folder_paths.append(per_worker_temporary_folder_path)

        maximum_buffer_size_in_bytes_per_worker = maximum_buffer_size_in_bytes // maximum_number_of_workers

        futures = []
        with ProcessPoolExecutor(max_workers=maximum_number_of_workers) as executor:
            for raw_s3_log_file_path in daily_raw_s3_log_file_paths:
                futures.append(
                    executor.submit(
                        _multi_worker_parse_dandi_raw_s3_log,
                        maximum_number_of_workers=maximum_number_of_workers,
                        raw_s3_log_file_path=raw_s3_log_file_path,
                        temporary_folder_path=temporary_folder_path,
                        excluded_ips=excluded_ips,
                        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes_per_worker,
                    ),
                )

            progress_bar_iterable = tqdm.tqdm(
                iterable=as_completed(futures),
                desc=f"Parsing log files using {maximum_number_of_workers} workers...",
                total=len(daily_raw_s3_log_file_paths),
                position=0,
                leave=True,
                mininterval=2.0,
                smoothing=0,  # Use true historical average, not moving average since shuffling makes it more uniform
            )
            for future in progress_bar_iterable:
                future.result()  # This is the call that finally triggers the deployment to the workers

        print("\n\nParallel parsing complete!\n\n")

        for per_worker_temporary_folder_path in tqdm.tqdm(
            iterable=per_worker_temporary_folder_paths,
            desc="Merging results across workers...",
            total=len(per_worker_temporary_folder_paths),
            position=0,
            leave=True,
            mininterval=2.0,
        ):
            per_worker_parsed_s3_log_file_paths = list(per_worker_temporary_folder_path.iterdir())
            assert (
                len(per_worker_parsed_s3_log_file_paths) != 0
            ), f"No files found in {per_worker_temporary_folder_path}!"

            for per_worker_parsed_s3_log_file_path in tqdm.tqdm(
                iterable=per_worker_parsed_s3_log_file_paths,
                desc="Merging results per worker...",
                total=len(per_worker_parsed_s3_log_file_paths),
                position=1,
                leave=False,
                mininterval=2.0,
            ):
                merged_temporary_file_path = parsed_s3_log_folder_path / per_worker_parsed_s3_log_file_path.name

                parsed_s3_log = pandas.read_table(filepath_or_buffer=per_worker_parsed_s3_log_file_path, header=0)

                header = False if merged_temporary_file_path.exists() else True
                parsed_s3_log.to_csv(
                    path_or_buf=merged_temporary_file_path,
                    mode="a",
                    sep="\t",
                    header=header,
                    index=False,
                )

            print("\n\n")


# Function cannot be covered because the line calls occur on subprocesses
# pragma: no cover
def _multi_worker_parse_dandi_raw_s3_log(
    *,
    maximum_number_of_workers: int,
    raw_s3_log_file_path: pathlib.Path,
    temporary_folder_path: pathlib.Path,
    excluded_ips: collections.defaultdict[str, bool] | None,
    maximum_buffer_size_in_bytes: int,
) -> None:
    """
    A mostly pass-through function to calculate the worker index on the worker and target the correct subfolder.

    Also dumps error stack (which is only typically seen by the worker and not sent back to the main stdout pipe)
    to a log file.
    """
    try:
        error_message = ""

        asset_id_handler = _get_default_dandi_asset_id_handler()

        worker_index = os.getpid() % maximum_number_of_workers
        per_worker_temporary_folder_path = temporary_folder_path / f"worker_{worker_index}"

        # Define error catching stuff as part of the try clause
        # so that if there is a problem within that, it too is caught
        errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
        errors_folder_path.mkdir(exist_ok=True)

        dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
        date = datetime.datetime.now().strftime("%y%m%d")
        parallel_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_parallel_errors.txt"
        error_message += (
            f"Worker index {worker_index}/{maximum_number_of_workers} parsing {raw_s3_log_file_path} failed due to\n\n"
        )

        parse_dandi_raw_s3_log(
            raw_s3_log_file_path=raw_s3_log_file_path,
            parsed_s3_log_folder_path=per_worker_temporary_folder_path,
            mode="a",
            excluded_ips=excluded_ips,
            asset_id_handler=asset_id_handler,
            tqdm_kwargs=dict(
                position=worker_index + 1, leave=False, desc=f"Parsing line buffers on worker {worker_index+1}..."
            ),
            maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        )
    except Exception as exception:
        with open(file=parallel_errors_file_path, mode="a") as io:
            error_message += f"{type(exception)}: {exception!s}\n\n{traceback.format_exc()}\n\n"
            io.write(error_message)


def parse_dandi_raw_s3_log(
    *,
    raw_s3_log_file_path: str | pathlib.Path,
    parsed_s3_log_folder_path: str | pathlib.Path,
    mode: Literal["w", "a"] = "a",
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
    asset_id_handler = asset_id_handler or _get_default_dandi_asset_id_handler()
    tqdm_kwargs = tqdm_kwargs or dict()

    bucket = "dandiarchive"
    request_type = "GET"

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
    )


def _get_default_dandi_asset_id_handler() -> Callable:
    def asset_id_handler(*, raw_asset_id: str) -> str:
        split_by_slash = raw_asset_id.split("/")
        return split_by_slash[0] + "_" + split_by_slash[-1]

    return asset_id_handler
