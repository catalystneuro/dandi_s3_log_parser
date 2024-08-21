"""Call the DANDI S3 log parser from the command line."""

import collections
import pathlib

import click

from ._bin_all_reduced_s3_logs_by_object_key import bin_all_reduced_s3_logs_by_object_key
from ._dandi_s3_log_file_reducer import (
    reduce_all_dandi_raw_s3_logs,
)
from ._map_all_reduced_s3_logs_to_dandisets import map_all_reduced_s3_logs_to_dandisets


@click.command(name="reduce_all_dandi_raw_s3_logs")
@click.option(
    "--raw_s3_logs_folder_path",
    help="The path to the folder containing all raw S3 log files.",
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
    "--excluded_years",
    help="A comma-separated list of years to exclude from parsing.",
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
def _reduce_all_dandi_raw_s3_logs_cli(
    raw_s3_logs_folder_path: str,
    reduced_s3_logs_folder_path: str,
    maximum_number_of_workers: int,
    maximum_buffer_size_in_mb: int,
    excluded_years: str | None,
    excluded_ips: str | None,
) -> None:
    split_excluded_years = excluded_years.split(",") if excluded_years is not None else []
    split_excluded_ips = excluded_ips.split(",") if excluded_ips is not None else []
    handled_excluded_ips = collections.defaultdict(bool) if len(split_excluded_ips) != 0 else None
    for excluded_ip in split_excluded_ips:
        handled_excluded_ips[excluded_ip] = True
    maximum_buffer_size_in_bytes = maximum_buffer_size_in_mb * 10**6

    reduce_all_dandi_raw_s3_logs(
        raw_s3_logs_folder_path=raw_s3_logs_folder_path,
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        maximum_number_of_workers=maximum_number_of_workers,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        excluded_years=split_excluded_years,
        excluded_ips=handled_excluded_ips,
    )

    return None


@click.command(name="bin_all_reduced_s3_logs_by_object_key")
@click.option(
    "--reduced_s3_logs_folder_path",
    help="The path to the folder containing all raw S3 log files.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--binned_s3_logs_folder_path",
    help="The path to write each reduced S3 log file to. There will be one file per handled asset ID.",
    required=True,
    type=click.Path(writable=True),
)
def _bin_all_reduced_s3_logs_by_object_key_cli(
    reduced_s3_logs_folder_path: str,
    binned_s3_logs_folder_path: str,
) -> None:
    bin_all_reduced_s3_logs_by_object_key(
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        binned_s3_logs_folder_path=binned_s3_logs_folder_path,
    )

    return None


@click.command(name="map_all_reduced_s3_logs_to_dandisets")
@click.option(
    "--binned_s3_logs_folder_path",
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
def _map_all_reduced_s3_logs_to_dandisets_cli(
    binned_s3_logs_folder_path: pathlib.Path, dandiset_logs_folder_path: pathlib.Path
) -> None:
    map_all_reduced_s3_logs_to_dandisets(
        binned_s3_logs_folder_path=binned_s3_logs_folder_path, dandiset_logs_folder_path=dandiset_logs_folder_path
    )

    return None
