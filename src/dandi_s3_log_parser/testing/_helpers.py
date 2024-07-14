"""Collection of helper functions related to testing and generating of example lines."""

import collections
import pathlib
import random
from typing import Literal

from .._config import REQUEST_TYPES


def find_random_example_line(
    raw_s3_log_folder_path: str | pathlib.Path,
    request_type: Literal[REQUEST_TYPES],
    maximum_lines_per_request_type: int = 5,
    seed: int = 0,
) -> str:
    """
    Return a randomly chosen line from a folder of raw S3 log files to serve as an example for testing purposes.

    Parameters
    ----------
    raw_s3_log_folder_path : string | pathlib.Path
        The path to the folder containing the raw S3 log files.
    request_type : string
        The type of request to filter for.
    maximum_lines_per_request_type : integer
        The maximum number of lines to randomly sample for each request type.
        The default is 5.

        These lines are always found chronologically from the start of the file.
    seed : int
        The seed to use for the random number generator.
    """
    raw_s3_log_folder_path = pathlib.Path(raw_s3_log_folder_path)

    all_raw_s3_log_file_paths = list(raw_s3_log_folder_path.rglob(pattern="*.log"))

    random.seed(seed)
    random.shuffle(all_raw_s3_log_file_paths)

    lines_by_request_type = collections.defaultdict(list)
    running_counts_by_request_type = collections.defaultdict(int)

    # Unsafe - but possibly faster
    # for random_log_file_path in all_raw_s3_log_file_paths:
    #     with open(file=random_log_file_path, mode="r") as io:
    #         all_lines = io.readlines()
    #
    #     # 170 is just an estimation
    #     # Unsafe: Might not have parsed 7+ elements from " " split
    #     # Unsafe: Might not have parsed 1+ elements from second "." split
    #     estimated_request_type_by_line = [line[:170].split(" ")[7].split(".")[1] for line in all_lines]
    #     for line_index, estimated_request_type in enumerate(estimated_request_type_by_line):
    #         lines_by_request_type[estimated_request_type].append(all_lines[line_index])
    #         running_counts_by_request_type[estimated_request_type] += 1
    #
    #         if running_counts_by_request_type[request_type] > maximum_lines_per_request_type:
    #             break
    #     if running_counts_by_request_type[request_type] > maximum_lines_per_request_type:
    #         break

    # Safe - but possibly slower
    for random_log_file_path in all_raw_s3_log_file_paths:
        with open(file=random_log_file_path, mode="r") as io:
            all_lines = io.readlines()

        # 170 is just an estimation
        sublines_items = [line[:170].split(" ") for line in all_lines]
        for line_index, subline_items in enumerate(sublines_items):
            # If line is as expected, some type of REST query should be at index 7
            if len(subline_items) < 8:
                continue

            # Result at this point should appear as something like 'REST.GET.OBJECT'
            raw_request_line = subline_items[7].split(".")
            if len(raw_request_line) != 3:
                raise ValueError(f"Bad request line found: {raw_request_line}")
            estimated_request_type = raw_request_line[1]

            lines_by_request_type[estimated_request_type].append(all_lines[line_index])
            running_counts_by_request_type[estimated_request_type] += 1

            if running_counts_by_request_type[request_type] > maximum_lines_per_request_type:
                break

        print(
            f"No lines found for request type ('{request_type}') in file '{random_log_file_path}'! "
            "Scanning the next file..."
        )

        if running_counts_by_request_type[request_type] > maximum_lines_per_request_type:
            break

    random_line = random.choice(seq=lines_by_request_type[request_type])

    # Replace IP information with placeholders
    random_line_items = random_line.split(" ")
    random_line_items[4] = "192.0.2.0"

    anonymized_random_line = " ".join(random_line_items)

    return anonymized_random_line
