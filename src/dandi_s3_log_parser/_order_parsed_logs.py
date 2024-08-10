import pathlib
import pandas


def order_parsed_logs(
    unordered_parsed_s3_log_folder_path: pathlib.Path, ordered_parsed_s3_log_folder_path: pathlib.Path
) -> None:
    """Order the contents of all parsed log files chronologically."""
    ordered_parsed_s3_log_folder_path.mkdir(exist_ok=True)

    for unordered_parsed_s3_log_file_path in unordered_parsed_s3_log_folder_path.iterdir():
        unordered_parsed_s3_log = pandas.read_table(filepath_or_buffer=unordered_parsed_s3_log_file_path, header=0)
        ordered_parsed_s3_log = unordered_parsed_s3_log.sort_values(by="timestamp")

        # correct index of first column
        ordered_parsed_s3_log.index = range(len(ordered_parsed_s3_log))

        ordered_parsed_s3_log_file_path = ordered_parsed_s3_log_folder_path / unordered_parsed_s3_log_file_path.name
        ordered_parsed_s3_log.to_csv(path_or_buf=ordered_parsed_s3_log_file_path, sep="\t", header=True, index=True)
