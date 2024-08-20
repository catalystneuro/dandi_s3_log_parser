"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import datetime
import os
import random
import traceback
import uuid
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed

import tqdm
from pydantic import DirectoryPath, Field, FilePath, validate_call

from ._error_collection import _collect_error
from ._s3_log_file_reducer import reduce_raw_s3_log


@validate_call
def reduce_all_dandi_raw_s3_logs(
    *,
    raw_s3_logs_folder_path: DirectoryPath,
    reduced_s3_logs_folder_path: DirectoryPath,
    maximum_number_of_workers: int = Field(ge=1, default=1),
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    excluded_years: list[str] | None = None,
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
    raw_s3_logs_folder_path : file path
        The path to the folder containing the raw S3 log files to be reduced.
    reduced_s3_logs_folder_path : file path
        The path to write each reduced S3 log file to.
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
    excluded_years = excluded_years or []
    excluded_ips = excluded_ips or collections.defaultdict(bool)

    object_key_handler = _get_default_dandi_object_key_handler()

    # Ensure all subfolders exist once at the start
    years_to_reduce = set([str(year) for year in range(2019, int(datetime.datetime.now().strftime("%Y")))]) - set(
        excluded_years
    )
    for year in years_to_reduce:
        reduced_year_path = reduced_s3_logs_folder_path / year
        reduced_year_path.mkdir(exist_ok=True)

        for month in range(1, 13):
            reduced_month_path = reduced_s3_logs_folder_path / str(month).zfill(2)
            reduced_month_path.mkdir(exist_ok=True)

    relative_s3_log_file_paths = [
        raw_s3_log_file_path.relative_to(raw_s3_logs_folder_path)
        for raw_s3_log_file_path in raw_s3_logs_folder_path.rglob(pattern="*.log")
        if raw_s3_log_file_path.stem.isdigit()
    ]
    relative_s3_log_file_paths_to_reduce = [
        relative_s3_log_file_path
        for relative_s3_log_file_path in relative_s3_log_file_paths
        if not (reduced_s3_logs_folder_path / relative_s3_log_file_path).exists()
    ]

    # The .rglob is not naturally sorted; shuffle for more uniform progress updates
    random.shuffle(relative_s3_log_file_paths_to_reduce)

    fields_to_reduce = ["object_key", "timestamp", "bytes_sent", "ip_address"]
    object_key_parents_to_reduce = ["blobs", "zarr"]
    line_buffer_tqdm_kwargs = dict(position=1, leave=False)
    # TODO: add better reporting units to all TQDMs (lines / s, files / s, etc.)
    if maximum_number_of_workers == 1:
        for relative_s3_log_file_path in tqdm.tqdm(
            iterable=relative_s3_log_file_paths_to_reduce,
            total=len(relative_s3_log_file_paths_to_reduce),
            desc="Parsing log files...",
            position=0,
            leave=True,
            smoothing=0,  # Use true historical average, not moving average since shuffling makes it more uniform
        ):
            raw_s3_log_file_path = raw_s3_logs_folder_path / relative_s3_log_file_path
            reduced_s3_log_file_path = reduced_s3_logs_folder_path / relative_s3_log_file_path

            reduce_raw_s3_log(
                raw_s3_log_file_path=raw_s3_log_file_path,
                reduced_s3_log_file_path=reduced_s3_log_file_path,
                fields_to_reduce=fields_to_reduce,
                object_key_parents_to_reduce=object_key_parents_to_reduce,
                maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
                excluded_ips=excluded_ips,
                object_key_handler=object_key_handler,
                line_buffer_tqdm_kwargs=line_buffer_tqdm_kwargs,
            )
    else:
        maximum_buffer_size_in_bytes_per_worker = maximum_buffer_size_in_bytes // maximum_number_of_workers

        futures = []
        with ProcessPoolExecutor(max_workers=maximum_number_of_workers) as executor:
            for relative_s3_log_file_path in relative_s3_log_file_paths_to_reduce:
                raw_s3_log_file_path = raw_s3_logs_folder_path / relative_s3_log_file_path
                reduced_s3_log_file_path = reduced_s3_logs_folder_path / relative_s3_log_file_path

                futures.append(
                    executor.submit(
                        _multi_worker_reduce_dandi_raw_s3_log,
                        raw_s3_log_file_path=raw_s3_log_file_path,
                        reduced_s3_log_file_path=reduced_s3_log_file_path,
                        fields_to_reduce=fields_to_reduce,
                        object_key_parents_to_reduce=object_key_parents_to_reduce,
                        maximum_number_of_workers=maximum_number_of_workers,
                        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes_per_worker,
                        excluded_ips=excluded_ips,
                    ),
                )

            progress_bar_iterable = tqdm.tqdm(
                iterable=as_completed(futures),
                total=len(futures),
                desc=f"Parsing log files using {maximum_number_of_workers} workers...",
                position=0,
                leave=True,
                mininterval=3.0,
                smoothing=0,  # Use true historical average, not moving average since shuffling makes it more uniform
            )
            for future in progress_bar_iterable:
                future.result()  # This is the call that finally triggers the deployment to the workers

    return None


# Function cannot be covered because the line calls occur on subprocesses
# pragma: no cover
def _multi_worker_reduce_dandi_raw_s3_log(
    *,
    raw_s3_log_file_path: FilePath,
    reduced_s3_log_file_path: FilePath,
    maximum_number_of_workers: int,
    maximum_buffer_size_in_bytes: int,
    excluded_ips: collections.defaultdict[str, bool],
) -> None:
    """
    A mostly pass-through function to calculate the worker index on the worker and target the correct subfolder.

    Also dumps error stack (which is only typically seen by the worker and not sent back to the main stdout pipe)
    to a log file.
    """
    try:
        worker_index = os.getpid() % maximum_number_of_workers

        fields_to_reduce = ["object_key", "timestamp", "bytes_sent", "ip_address"]
        object_key_parents_to_reduce = ["blobs", "zarr"]
        object_key_handler = _get_default_dandi_object_key_handler()
        line_buffer_tqdm_kwargs = dict(
            position=worker_index + 1, leave=False, desc=f"Parsing line buffers on worker {worker_index + 1}..."
        )

        reduce_raw_s3_log(
            raw_s3_log_file_path=raw_s3_log_file_path,
            reduced_s3_log_file_path=reduced_s3_log_file_path,
            fields_to_reduce=fields_to_reduce,
            object_key_parents_to_reduce=object_key_parents_to_reduce,
            maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
            excluded_ips=excluded_ips,
            object_key_handler=object_key_handler,
            line_buffer_tqdm_kwargs=line_buffer_tqdm_kwargs,
        )
    except Exception as exception:
        message = (
            f"Worker index {worker_index}/{maximum_number_of_workers} reducing {raw_s3_log_file_path} failed!\n\n"
            f"{type(exception)}: {exception}\n\n"
            f"{traceback.format_exc()}"
        )
        task_id = str(uuid.uuid4())[:5]
        _collect_error(message=message, error_type="parallel", task_id=task_id)

    return None


def _get_default_dandi_object_key_handler() -> Callable:
    def object_key_handler(*, object_key: str) -> str:
        split_by_slash = object_key.split("/")

        object_type = split_by_slash[0]
        if object_type == "zarr":
            zarr_blob_form = "/".join(split_by_slash[:2])
            return zarr_blob_form

        return object_key

    return object_key_handler
