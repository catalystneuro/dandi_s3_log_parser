"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
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

from ._error_collection import _collect_error
from ._s3_log_file_reducer import reduce_raw_s3_log


@validate_call
def reduce_all_dandi_raw_s3_logs(
    *,
    base_raw_s3_logs_folder_path: DirectoryPath,
    reduced_s3_logs_folder_path: DirectoryPath,
    maximum_number_of_workers: int = Field(ge=1, default=1),
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    excluded_ips: collections.defaultdict[str, bool] | None = None,
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
    base_raw_s3_logs_folder_path : file path
        The Path to the folder containing the raw S3 log files to be reduced.
    reduced_s3_logs_folder_path : file path
        The Path to write each reduced S3 log file to.
        There will be one file per handled asset ID.
    maximum_number_of_workers : int, default: 1
        The maximum number of workers to distribute tasks across.
    maximum_buffer_size_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to use on each buffer iteration when reading from the
        source text files.

        Actual total RAM usage will be higher due to overhead and caching.

        Automatically splits this total amount over the maximum number of workers if `maximum_number_of_workers` is
        greater than one.
    excluded_ips : collections.defaultdict(bool), optional
        A lookup table whose keys are IP addresses to exclude from reduction.
    """
    excluded_ips = excluded_ips or collections.defaultdict(bool)

    asset_id_handler = _get_default_dandi_asset_id_handler()

    daily_raw_s3_log_file_paths = [
        path for path in base_raw_s3_logs_folder_path.rglob(pattern="*.log") if path.stem.isdigit()
    ]

    # The .rglob is not naturally sorted; shuffle for more uniform progress updates
    random.shuffle(daily_raw_s3_log_file_paths)

    # TODO: add better reporting units to all TQDMs (lines / s, files / s, etc.)
    if maximum_number_of_workers == 1:
        for raw_s3_log_file_path in tqdm.tqdm(
            iterable=daily_raw_s3_log_file_paths,
            desc="Parsing log files...",
            position=0,
            leave=True,
            smoothing=0,  # Use true historical average, not moving average since shuffling makes it more uniform
        ):
            reduce_dandi_raw_s3_log(
                raw_s3_log_file_path=raw_s3_log_file_path,
                reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
                mode="a",
                maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
                excluded_ips=excluded_ips,
                asset_id_handler=asset_id_handler,
                tqdm_kwargs=dict(position=1, leave=False),
            )
    else:
        # Create a fresh temporary directory in the home folder and then fresh subfolders for each worker
        temporary_base_folder_path = reduced_s3_logs_folder_path / ".temp"
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
                        _multi_worker_reduce_dandi_raw_s3_log,
                        raw_s3_log_file_path=raw_s3_log_file_path,
                        temporary_folder_path=temporary_folder_path,
                        maximum_number_of_workers=maximum_number_of_workers,
                        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes_per_worker,
                        excluded_ips=excluded_ips,
                    ),
                )

            progress_bar_iterable = tqdm.tqdm(
                iterable=as_completed(futures),
                desc=f"Parsing log files using {maximum_number_of_workers} workers...",
                total=len(daily_raw_s3_log_file_paths),
                position=0,
                leave=True,
                mininterval=3.0,
                smoothing=0,  # Use true historical average, not moving average since shuffling makes it more uniform
            )
            for future in progress_bar_iterable:
                future.result()  # This is the call that finally triggers the deployment to the workers

        print("\n\nParallel parsing complete!\n\n")

        for worker_index, per_worker_temporary_folder_path in enumerate(
            tqdm.tqdm(
                iterable=per_worker_temporary_folder_paths,
                desc="Merging results across workers...",
                total=len(per_worker_temporary_folder_paths),
                position=0,
                leave=True,
                mininterval=3.0,
            )
        ):
            per_worker_reduced_s3_log_file_paths = list(per_worker_temporary_folder_path.rglob("*.tsv"))
            assert (
                len(per_worker_reduced_s3_log_file_paths) != 0
            ), f"No files found in {per_worker_temporary_folder_path}!"

            for per_worker_reduced_s3_log_file_path in tqdm.tqdm(
                iterable=per_worker_reduced_s3_log_file_paths,
                desc="Merging results per worker...",
                total=len(per_worker_reduced_s3_log_file_paths),
                position=1,
                leave=False,
                mininterval=3.0,
            ):
                merge_target_file_path = reduced_s3_logs_folder_path / per_worker_reduced_s3_log_file_path.relative_to(
                    per_worker_temporary_folder_path
                )

                parsed_s3_log = pandas.read_table(filepath_or_buffer=per_worker_reduced_s3_log_file_path, header=0)

                merge_target_file_path_exists = merge_target_file_path.exists()
                if not merge_target_file_path_exists and not merge_target_file_path.parent.exists():
                    merge_target_file_path.parent.mkdir(exist_ok=True, parents=True)

                header = False if merge_target_file_path_exists else True
                parsed_s3_log.to_csv(
                    path_or_buf=merge_target_file_path,
                    mode="a",
                    sep="\t",
                    header=header,
                    index=False,
                )

        shutil.rmtree(path=temporary_base_folder_path)


# Function cannot be covered because the line calls occur on subprocesses
# pragma: no cover
def _multi_worker_reduce_dandi_raw_s3_log(
    *,
    raw_s3_log_file_path: pathlib.Path,
    temporary_folder_path: pathlib.Path,
    maximum_number_of_workers: int,
    maximum_buffer_size_in_bytes: int,
    excluded_ips: collections.defaultdict[str, bool] | None,
) -> None:
    """
    A mostly pass-through function to calculate the worker index on the worker and target the correct subfolder.

    Also dumps error stack (which is only typically seen by the worker and not sent back to the main stdout pipe)
    to a log file.
    """
    try:
        asset_id_handler = _get_default_dandi_asset_id_handler()

        worker_index = os.getpid() % maximum_number_of_workers
        per_worker_temporary_folder_path = temporary_folder_path / f"worker_{worker_index}"

        reduce_dandi_raw_s3_log(
            raw_s3_log_file_path=raw_s3_log_file_path,
            reduced_s3_logs_folder_path=per_worker_temporary_folder_path,
            mode="a",
            maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
            excluded_ips=excluded_ips,
            asset_id_handler=asset_id_handler,
            tqdm_kwargs=dict(
                position=worker_index + 1, leave=False, desc=f"Parsing line buffers on worker {worker_index+1}..."
            ),
        )
    except Exception as exception:
        message = (
            f"Worker index {worker_index}/{maximum_number_of_workers} reducing {raw_s3_log_file_path} failed!\n\n"
            f"{type(exception)}: {exception!s}\n\n"
            f"{traceback.format_exc()}"
        )
        task_id = str(uuid.uuid4())[:5]
        _collect_error(message=message, error_type="parallel", task_id=task_id)


@validate_call
def reduce_dandi_raw_s3_log(
    *,
    raw_s3_log_file_path: FilePath,
    reduced_s3_logs_folder_path: DirectoryPath,
    mode: Literal["w", "a"] = "a",
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    asset_id_handler: Callable | None = None,
    tqdm_kwargs: dict | None = None,
) -> None:
    """
    Reduce a raw S3 log file and write the results to a folder of TSV files, one for each unique asset ID.

    'Parsing' here means:
      - limiting only to requests of the specified type (i.e., GET, PUT, etc.)
      - reducing the information to the asset ID, request time, request size, and geographic IP of the requester

    Parameters
    ----------
    raw_s3_log_file_path : string or pathlib.Path
        Path to the raw S3 log file to be reduced.
    reduced_s3_logs_folder_path : string or pathlib.Path
        The path to write each reduced S3 log file to.
        There will be one file per handled asset ID.
    mode : "w" or "a", default: "a"
        How to resolve the case when files already exist in the folder containing parsed logs.
        "w" will overwrite existing content, "a" will append or create if the file does not yet exist.

        The intention of the default usage is to have one consolidated raw S3 log file per day and then to iterate
        over each day, parsing and binning by asset, effectively 'updating' the parsed collection on each iteration.
    maximum_buffer_size_in_bytes : int, default: 4 GB
        The theoretical maximum amount of RAM (in bytes) to use on each buffer iteration when reading from the
        source text file.

        Actual RAM usage will be higher due to overhead and caching.
    excluded_ips : collections.defaultdict(bool), optional
        A lookup table whose keys are IP addresses to exclude from reduction.
    asset_id_handler : callable, optional
        If your asset IDs in the raw log require custom handling (i.e., they contain slashes that you do not wish to
        translate into nested directory paths) then define a function of the following form:

        # For example
        def asset_id_handler(*, raw_asset_id: str) -> str:
            split_by_slash = raw_asset_id.split("/")
            return split_by_slash[0] + "_" + split_by_slash[-1]
    tqdm_kwargs : dict, optional
        Keyword arguments to pass to the tqdm progress bar for line buffers.
    """
    raw_s3_log_file_path = pathlib.Path(raw_s3_log_file_path)
    reduced_s3_logs_folder_path = pathlib.Path(reduced_s3_logs_folder_path)
    asset_id_handler = asset_id_handler or _get_default_dandi_asset_id_handler()
    tqdm_kwargs = tqdm_kwargs or dict()

    bucket = "dandiarchive"
    operation_type = "REST.GET.OBJECT"

    reduce_raw_s3_log(
        raw_s3_log_file_path=raw_s3_log_file_path,
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        mode=mode,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        bucket=bucket,
        operation_type=operation_type,
        excluded_ips=excluded_ips,
        asset_id_handler=asset_id_handler,
        tqdm_kwargs=tqdm_kwargs,
    )


def _get_default_dandi_asset_id_handler() -> Callable:
    def asset_id_handler(*, raw_asset_id: str) -> str:
        split_by_slash = raw_asset_id.split("/")

        asset_type = split_by_slash[0]
        if asset_type == "zarr":
            zarr_blob_form = "/".join(split_by_slash[:2])
            return zarr_blob_form

        return raw_asset_id

    return asset_id_handler
