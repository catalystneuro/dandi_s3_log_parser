import pathlib
import random

import tqdm
from pydantic import DirectoryPath, FilePath, validate_call

from ._buffered_text_reader import BufferedTextReader


@validate_call
def find_all_known_operation_types(
    base_raw_s3_log_folder_path: DirectoryPath,
    excluded_log_files: list[FilePath] | None,
    max_files: int | None = 100,
) -> set:
    base_raw_s3_log_folder_path = pathlib.Path(base_raw_s3_log_folder_path)
    excluded_log_files = excluded_log_files or {}
    excluded_log_files = {pathlib.Path(excluded_log_file) for excluded_log_file in excluded_log_files}

    daily_raw_s3_log_file_paths = list(set(base_raw_s3_log_folder_path.rglob(pattern="*.log")) - excluded_log_files)
    random.shuffle(daily_raw_s3_log_file_paths)

    unique_operation_types = set()
    for raw_s3_log_file_path in tqdm.tqdm(
        iterable=daily_raw_s3_log_file_paths[:max_files],
        desc="Extracting operation types from log files...",
        position=0,
        leave=True,
    ):
        operation_types_per_file = {
            raw_log_line[:180].split(" ")[7]
            for buffered_text_reader in BufferedTextReader(file_path=raw_s3_log_file_path)
            for raw_log_line in buffered_text_reader
        }

        unique_operation_types.update(operation_types_per_file)

    return unique_operation_types
