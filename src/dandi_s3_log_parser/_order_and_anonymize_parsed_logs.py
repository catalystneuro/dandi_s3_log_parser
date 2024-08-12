import pathlib

import pandas
import tqdm


def order_and_anonymize_parsed_logs(
    unordered_parsed_s3_log_folder_path: pathlib.Path, ordered_and_anonymized_s3_log_folder_path: pathlib.Path,
) -> None:
    """Order the contents of all parsed log files chronologically."""
    ordered_and_anonymized_s3_log_folder_path.mkdir(exist_ok=True)

    unordered_file_paths = list(unordered_parsed_s3_log_folder_path.glob("*.tsv"))
    for unordered_parsed_s3_log_file_path in tqdm.tqdm(
        iterable=unordered_file_paths,
        total=len(unordered_file_paths),
        desc="Ordering parsed logs...",
        position=0,
        leave=True,
        mininterval=3.0,
    ):
        unordered_parsed_s3_log = pandas.read_table(filepath_or_buffer=unordered_parsed_s3_log_file_path, header=0)
        ordered_and_anonymized_parsed_s3_log = unordered_parsed_s3_log.sort_values(by="timestamp")

        # correct index of first column
        ordered_and_anonymized_parsed_s3_log.index = range(len(unordered_parsed_s3_log))

        ordered_and_anonymized_parsed_s3_log_file_path = (
            ordered_and_anonymized_s3_log_folder_path / unordered_parsed_s3_log_file_path.name
        )
        ordered_and_anonymized_parsed_s3_log.to_csv(
            path_or_buf=ordered_and_anonymized_parsed_s3_log_file_path, sep="\t", header=True, index=True,
        )
