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
        "The default is 5. \n"
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
