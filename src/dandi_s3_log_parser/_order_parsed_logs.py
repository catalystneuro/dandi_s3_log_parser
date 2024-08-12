import pathlib
import datetime
import traceback
import importlib.metadata

import pandas

from ._config import DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH


def order_parsed_logs(
    unordered_parsed_s3_log_folder_path: pathlib.Path, ordered_parsed_s3_log_folder_path: pathlib.Path
) -> None:
    """Order the contents of all parsed log files chronologically."""
    ordered_parsed_s3_log_folder_path.mkdir(exist_ok=True)

    errors_folder_path = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    errors_folder_path.mkdir(exist_ok=True)

    dandi_s3_log_parser_version = importlib.metadata.version(distribution_name="dandi_s3_log_parser")
    date = datetime.datetime.now().strftime("%y%m%d")
    ordering_errors_file_path = errors_folder_path / f"v{dandi_s3_log_parser_version}_{date}_ordering_errors.txt"

    for unordered_parsed_s3_log_file_path in unordered_parsed_s3_log_folder_path.glob("*.tsv"):
        try:
            error_message = f"Ordering {unordered_parsed_s3_log_file_path}...\n\n"

            unordered_parsed_s3_log = pandas.read_table(filepath_or_buffer=unordered_parsed_s3_log_file_path, header=0)
            ordered_parsed_s3_log = unordered_parsed_s3_log.sort_values(by="timestamp")

            # correct index of first column
            ordered_parsed_s3_log.index = range(len(ordered_parsed_s3_log))

            ordered_parsed_s3_log_file_path = ordered_parsed_s3_log_folder_path / unordered_parsed_s3_log_file_path.name
            ordered_parsed_s3_log.to_csv(path_or_buf=ordered_parsed_s3_log_file_path, sep="\t", header=True, index=True)
        except Exception as exception:
            with open(file=ordering_errors_file_path, mode="a") as io:
                error_message += f"{type(exception)}: {str(exception)}\n\n{traceback.format_exc()}\n\n"
                io.write(error_message)
