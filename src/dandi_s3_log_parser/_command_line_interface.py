"""Call the raw S3 log parser from the command line."""

import pathlib
import click
from typing import Literal

from ._s3_log_file_parser import parse_dandi_raw_s3_log
from .testing._helpers import find_random_example_line
from ._config import REQUEST_TYPES


# TODO
@click.command(name="parse_dandi_raw_s3_logs")
def parse_dandi_raw_s3_log_cli() -> None:
    parse_dandi_raw_s3_log()


@click.command(name="find_random_example_line")
@click.option(
    "--raw_s3_log_folder_path",
    required=True,
    type=click.Path(writable=True),
    help="The path to the folder containing the raw S3 log files.",
)
@click.option(
    "--request_type", required=True, type=click.Choice(REQUEST_TYPES), help="The type of request to filter for."
)
@click.option("--seed", required=True, type=int, help="The seed to use for the random number generator.")
def find_random_example_line_cli(
    raw_s3_log_folder_path: str | pathlib.Path, request_type: Literal[REQUEST_TYPES], seed: int = 0
) -> None:
    """Find a randomly chosen line from a folder of raw S3 log files to serve as an example for testing purposes."""
    example_line = find_random_example_line(
        raw_s3_log_folder_path=raw_s3_log_folder_path, request_type=request_type, seed=seed
    )
    print(example_line)
