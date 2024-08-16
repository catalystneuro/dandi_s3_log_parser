"""Call the DANDI S3 log parser from the command line."""

import collections
import pathlib
from typing import Literal

import click

from ._config import REQUEST_TYPES
from ._dandi_s3_log_file_reducer import (
    reduce_all_dandi_raw_s3_logs,
    reduce_dandi_raw_s3_log,
)
from ._dandiset_mapper import map_reduced_logs_to_dandisets
from .testing import find_random_example_line


@click.command(name="reduce_all_dandi_raw_s3_logs")
@click.option(
    "--base_raw_s3_logs_folder_path",
    help="The path to the base folder containing all raw S3 log files.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--reduced_s3_logs_folder_path",
    help="The path to write each reduced S3 log file to. There will be one file per handled asset ID.",
    required=True,
    type=click.Path(writable=True),
)
@click.option(
    "--maximum_number_of_workers",
    help="The maximum number of workers to distribute tasks across.",
    required=False,
    type=click.IntRange(min=1),
    default=1,
)
@click.option(
    "--maximum_buffer_size_in_mb",
    help=(
        "The theoretical maximum amount of RAM (in MB) to use on each buffer iteration when reading from the "
        "source text files. "
        "Actual total RAM usage will be higher due to overhead and caching. "
        "Automatically splits this total amount over the maximum number of workers if `maximum_number_of_workers` is "
        "greater than one."
    ),
    required=False,
    type=click.IntRange(min=1),  # Bare minimum of 1 MB
    default=1_000,  # 1 GB recommended
)
@click.option(
    "--excluded_ips",
    help="A comma-separated list of IP addresses to exclude from parsing.",
    required=False,
    type=str,
    default=None,
)
def _reduce_all_dandi_raw_s3_logs_cli(
    base_raw_s3_logs_folder_path: str,
    reduced_s3_logs_folder_path: str,
    maximum_number_of_workers: int,
    maximum_buffer_size_in_mb: int,
    excluded_ips: str | None,
) -> None:
    split_excluded_ips = excluded_ips.split(",") if excluded_ips is not None else list()
    handled_excluded_ips = collections.defaultdict(bool) if len(split_excluded_ips) != 0 else None
    for excluded_ip in split_excluded_ips:
        handled_excluded_ips[excluded_ip] = True
    maximum_buffer_size_in_bytes = maximum_buffer_size_in_mb * 10**6

    reduce_all_dandi_raw_s3_logs(
        base_raw_s3_logs_folder_path=base_raw_s3_logs_folder_path,
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        maximum_number_of_workers=maximum_number_of_workers,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        excluded_ips=handled_excluded_ips,
    )


@click.command(name="reduce_dandi_raw_s3_log")
@click.option(
    "--raw_s3_log_file_path",
    help="The path to the raw S3 log file to be reduced.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--reduced_s3_logs_folder_path",
    help="The path to write each reduced S3 log file to. There will be one file per handled asset ID.",
    required=True,
    type=click.Path(writable=True),
)
@click.option(
    "--maximum_buffer_size_in_mb",
    help=(
        "The theoretical maximum amount of RAM (in MB) to use on each buffer iteration when reading from the "
        "source text files. "
        "Actual total RAM usage will be higher due to overhead and caching. "
        "Automatically splits this total amount over the maximum number of workers if `maximum_number_of_workers` is "
        "greater than one."
    ),
    required=False,
    type=click.IntRange(min=1),  # Bare minimum of 1 MB
    default=1_000,  # 1 GB recommended
)
@click.option(
    "--excluded_ips",
    help="A comma-separated list of IP addresses to exclude from reduction.",
    required=False,
    type=str,
    default=None,
)
def _reduce_dandi_raw_s3_log_cli(
    raw_s3_log_file_path: str,
    reduced_s3_logs_folder_path: str,
    excluded_ips: str | None,
    maximum_buffer_size_in_mb: int,
) -> None:
    split_excluded_ips = excluded_ips.split(",") if excluded_ips is not None else list()
    handled_excluded_ips = collections.defaultdict(bool) if len(split_excluded_ips) != 0 else None
    for excluded_ip in split_excluded_ips:
        handled_excluded_ips[excluded_ip] = True
    maximum_buffer_size_in_bytes = maximum_buffer_size_in_mb * 10**6

    reduce_dandi_raw_s3_log(
        raw_s3_log_file_path=raw_s3_log_file_path,
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        excluded_ips=handled_excluded_ips,
    )


@click.command(name="map_reduced_logs_to_dandisets")
@click.option(
    "--reduced_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--dandiset_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
def _map_reduced_logs_to_dandisets_cli(
    reduced_s3_logs_folder_path: pathlib.Path, dandiset_logs_folder_path: pathlib.Path
) -> None:
    map_reduced_logs_to_dandisets(
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path, dandiset_logs_folder_path=dandiset_logs_folder_path
    )


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
        "The maximum number of lines to randomly sample for each request type. "
        "The default is 5. "
        "These lines are always found chronologically from the start of the file."
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
def _find_random_example_line_cli(
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

    return None
