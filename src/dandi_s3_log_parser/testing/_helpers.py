"""Collection of helper functions related to testing and generating of example lines."""

import collections
import pathlib
from typing import Literal


def find_random_example_line(
    raw_s3_log_folder_path: str | pathlib.Path, request_type: Literal["GET", "PUT", "HEAD"], seed: int = 0
) -> str:
    """Return a random example line from a folder of raw S3 log files."""
    raw_s3_log_folder_path = pathlib.Path(raw_s3_log_folder_path)

    all_raw_s3_log_file_paths = list(raw_s3_log_folder_path.rglob(pattern="*.log"))

    random.seed(seed)
    random_log_file_path = random.choice(seq=all_raw_s3_log_file_paths)

    with open(file=random_log_file_path, mode="r") as io:
        all_lines = io.readlines()

    lines_by_request_type = collections.defaultdict(list)
    for line in all_lines:
        subline = line[170]  # 170 is just an estimation
        subline_items = subline.split(" ")

        # If line is as expected, some type of REST query should be at index 7
        if len(subline_items) < 8:
            continue

        # Result at this point should appear as something like 'REST.GET.OBJECT'
        estimated_request_type = subline[7].split(".")[1]
        lines_by_request_type[estimated_request_type].append(line)

    random_line = random.choice(seq=lines_by_request_type[request_type])

    # Replace IP information with placeholders
    random_line_items = random_line.split(" ")
    random_line_items[4] = "192.0.2.0"

    anonymized_random_line = " ".join(random_line_items)
    return anonymized_random_line
