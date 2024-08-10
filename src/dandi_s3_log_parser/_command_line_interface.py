"""Call the raw S3 log parser from the command line."""

import collections
import os
import pathlib
import click
from typing import Literal

from ._s3_log_file_parser import parse_dandi_raw_s3_log, parse_all_dandi_raw_s3_logs
from .testing._helpers import find_random_example_line
from ._config import REQUEST_TYPES

NUMBER_OF_CPU = os.cpu_count()  # Note: Not distinguishing if logical or not


@click.command(name="parse_all_dandi_raw_s3_logs")
@click.option(
    "--base_raw_s3_log_folder_path",
    help="The path to the base folder containing all raw S3 log files.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--parsed_s3_log_folder_path",
    help="The path to write each parsed S3 log file to. There will be one file per handled asset ID.",
    required=True,
    type=click.Path(writable=True),
)
@click.option(
    "--mode",
    help=(
        """How to resolve the case when files already exist in the folder containing parsed logs.
"w" will overwrite existing content, "a" will append or create if the file does not yet exist.

The intention of the default usage is to have one consolidated raw S3 log file per day and then to iterate
over each day, parsing and binning by asset, effectively 'updating' the parsed collection on each iteration.
HINT: If this iteration is done in chronological order, the resulting parsed logs will also maintain that order.
"""
    ),
    required=False,
    type=click.Choice(["w", "a"]),
    default="a",
)
@click.option(
    "--excluded_ips",
    help="A comma-separated list of IP addresses to exclude from parsing.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--number_of_jobs",
    help="The number of jobs to use for parallel processing.",
    required=False,
    type=int,
    default=1,
)
def parse_all_dandi_raw_s3_logs_cli(
    base_raw_s3_log_folder_path: str,
    parsed_s3_log_folder_path: str,
    mode: Literal["w", "a"] = "a",
    excluded_ips: str | None = None,
    number_of_jobs: int = 1,
) -> None:
    number_of_jobs = NUMBER_OF_CPU + number_of_jobs + 1 if number_of_jobs < 0 else number_of_jobs
    assert number_of_jobs > 0, "The number of jobs must be greater than 0."
    assert number_of_jobs <= NUMBER_OF_CPU, "The number of jobs must be less than or equal to the number of CPUs."

    split_excluded_ips = excluded_ips.split(",") if excluded_ips is not None else []
    handled_excluded_ips = collections.defaultdict(bool) if len(split_excluded_ips) != 0 else None
    for excluded_ip in split_excluded_ips:
        handled_excluded_ips[excluded_ip] = True

    parse_all_dandi_raw_s3_logs(
        base_raw_s3_log_folder_path=base_raw_s3_log_folder_path,
        parsed_s3_log_folder_path=parsed_s3_log_folder_path,
        mode=mode,
        excluded_ips=handled_excluded_ips,
        number_of_jobs=number_of_jobs,
    )


# TODO
@click.command(name="parse_dandi_raw_s3_logs")
def parse_dandi_raw_s3_log_cli() -> None:
    parse_dandi_raw_s3_log()


@click.command(name="find_random_example_line")
@click.option(
    "--raw_s3_log_folder_path",
    help="The path to the folder containing the raw S3 log files.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--request_type",
    help="The type of request to filter for.",
    required=True,
    type=click.Choice(REQUEST_TYPES),
)
@click.option(
    "--maximum_lines_per_request_type",
    help=(
        """The maximum number of lines to randomly sample for each request type.
The default is 5.

These lines are always found chronologically from the start of the file.
"""
    ),
    required=False,
    type=click.IntRange(min=2),
    default=100,
)
@click.option(
    "--seed",
    help="The seed to use for the random number generator. The default is 0.",
    required=False,
    type=click.IntRange(min=0),
    default=0,
)
def find_random_example_line_cli(
    raw_s3_log_folder_path: str | pathlib.Path,
    request_type: Literal[REQUEST_TYPES],
    maximum_lines_per_request_type: int = 5,
    seed: int = 0,
) -> None:
    """Find a randomly chosen line from a folder of raw S3 log files to serve as an example for testing purposes."""
    example_line = find_random_example_line(
        raw_s3_log_folder_path=raw_s3_log_folder_path,
        request_type=request_type,
        maximum_lines_per_request_type=maximum_lines_per_request_type,
        seed=seed,
    )
    print(example_line)
