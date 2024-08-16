"""Primary functions for parsing raw S3 log file for DANDI."""

import collections
import datetime
import importlib.metadata
import pathlib
import uuid
from collections.abc import Callable
from typing import Literal

import pandas
import tqdm
from pydantic import FilePath, validate_call

from ._buffered_text_reader import BufferedTextReader
from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH
from ._s3_log_line_parser import _KNOWN_OPERATION_TYPES, _append_reduced_log_line


@validate_call
def reduce_raw_s3_log(
    *,
    raw_s3_log_file_path: FilePath,
    parsed_s3_log_folder_path: FilePath,
    mode: Literal["w", "a"] = "a",
    maximum_buffer_size_in_bytes: int = 4 * 10**9,
    bucket: str | None = None,
    operation_type: Literal[_KNOWN_OPERATION_TYPES] = "REST.GET.OBJECT",
    excluded_ips: collections.defaultdict[str, bool] | None = None,
    asset_id_handler: Callable | None = None,
    tqdm_kwargs: dict | None = None,
) -> None:
    """
    Reduce a raw S3 log file and write the results to a folder of TSV files, one for each unique asset ID.

    'Reduce' here means:
      - Filtering all lines only by the bucket specified.
      - Filtering all lines only by the type of operation specified (i.e., REST.GET.OBJECT, REST.PUT.OBJECT, etc.).
      - Filtering out any non-success status codes.
      - Filtering out any excluded IP addresses.
      - Extracting only the asset ID, request timestamp, request size, and IP address that sent the request.

    Parameters
    ----------
    raw_s3_log_file_path : str or pathlib.Path
        The path to the raw S3 log file.
    parsed_s3_log_folder_path : str or pathlib.Path
        The path to write each parsed S3 log file to.
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
    bucket : str
        Only parse and return lines that match this bucket.
    operation_type : str, default: "REST.GET"
        The type of operation to filter for.
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
        Keyword arguments to pass to the tqdm progress bar for line buffers.
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
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        bucket=bucket,
        operation_type=operation_type,
        excluded_ips=excluded_ips,
        asset_id_handler=asset_id_handler,
        tqdm_kwargs=tqdm_kwargs,
    )

    for handled_asset_id, reduced_logs_per_handled_asset_id in reduced_and_binned_logs.items():
        parsed_s3_log_file_path = parsed_s3_log_folder_path / f"{handled_asset_id}.tsv"

        data_frame = pandas.DataFrame(data=reduced_logs_per_handled_asset_id)

        header = False if parsed_s3_log_file_path.exists() is True and mode == "a" else True
        data_frame.to_csv(path_or_buf=parsed_s3_log_file_path, mode=mode, sep="\t", header=header, index=False)


def _get_reduced_and_binned_log_lines(
    *,
    raw_s3_log_file_path: pathlib.Path,
    maximum_buffer_size_in_bytes: int,
    bucket: str,
    operation_type: Literal[_KNOWN_OPERATION_TYPES],
    excluded_ips: collections.defaultdict[str, bool],
    asset_id_handler: Callable,
    tqdm_kwargs: dict,
) -> collections.defaultdict[str, dict[str, list[str | int]]]:
    """Reduce the full S3 log file to minimal content and bin by asset ID."""
    tqdm_kwargs = tqdm_kwargs or dict()
    default_tqdm_kwargs = dict(desc="Parsing line buffers...", leave=False)
    resolved_tqdm_kwargs = dict(default_tqdm_kwargs)
    resolved_tqdm_kwargs.update(tqdm_kwargs)

    errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    errors_folder_path.mkdir(exist_ok=True)

    dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
    date = datetime.datetime.now().strftime("%y%m%d")
    task_id = str(uuid.uuid4())[:5]
    lines_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_line_errors_{task_id}.txt"

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
                bucket=bucket,
                operation_type=operation_type,
                excluded_ips=excluded_ips,
                asset_id_handler=asset_id_handler,
                lines_errors_file_path=lines_errors_file_path,
                log_file_path=raw_s3_log_file_path,
                line_index=line_index,
                task_id=task_id,
            )
        per_buffer_index += index

    return reduced_and_binned_logs
