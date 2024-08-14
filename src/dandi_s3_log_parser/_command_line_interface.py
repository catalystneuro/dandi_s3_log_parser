"""Call the raw S3 log parser from the command line."""

import collections
import os
import pathlib
from typing import Literal

import click

from ._config import REQUEST_TYPES
from ._dandi_s3_log_file_parser import (
    parse_all_dandi_raw_s3_logs,
    parse_dandi_raw_s3_log,
)
from .testing._helpers import find_random_example_line

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
    "--excluded_log_files",
    help="A comma-separated list of log files to exclude from parsing.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--excluded_ips",
    help="A comma-separated list of IP addresses to exclude from parsing.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--maximum_number_of_workers",
    help="The maximum number of workers to distribute tasks across.",
    required=False,
    type=click.IntRange(min=1),
    default=1,
)
@click.option(
    "--maximum_buffer_size_in_MB",
    help=""""
The theoretical maximum amount of RAM (in MB) to use on each buffer iteration when reading from the
    source text files.
    Actual total RAM usage will be higher due to overhead and caching.
    Automatically splits this total amount over the maximum number of workers if `maximum_number_of_workers` is
    greater than one.
""",
    required=False,
    type=click.IntRange(min=1),  # Bare minimum of 1 MB
    default=1_000,  # 1 GB recommended
)
def parse_all_dandi_raw_s3_logs_cli(
    base_raw_s3_log_folder_path: str,
    parsed_s3_log_folder_path: str,
    excluded_log_files: str | None,
    excluded_ips: str | None,
    maximum_number_of_workers: int,
    maximum_buffer_size_in_mb: int,
) -> None:
    split_excluded_log_files = excluded_log_files.split(",") if excluded_log_files is not None else list()
    split_excluded_ips = excluded_ips.split(",") if excluded_ips is not None else list()
    handled_excluded_ips = collections.defaultdict(bool) if len(split_excluded_ips) != 0 else None
    for excluded_ip in split_excluded_ips:
        handled_excluded_ips[excluded_ip] = True
    maximum_buffer_size_in_bytes = maximum_buffer_size_in_mb * 10**6

    parse_all_dandi_raw_s3_logs(
        base_raw_s3_log_folder_path=base_raw_s3_log_folder_path,
        parsed_s3_log_folder_path=parsed_s3_log_folder_path,
        excluded_log_files=split_excluded_log_files,
        excluded_ips=handled_excluded_ips,
        maximum_number_of_workers=maximum_number_of_workers,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )


# TODO
@click.command(name="parse_dandi_raw_s3_log")
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
