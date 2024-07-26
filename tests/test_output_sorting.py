import pytest
import pathlib
import random
import calendar
import datetime

SEED = 0
random.seed(SEED)


def _generate_random_datetime() -> datetime.datetime:
    """Generate a random datetime for testing."""
    year = random.randint(2000, 2020)
    month = random.randint(1, 12)

    max_days = calendar.monthrange(year=year, month=month)[1]
    day = random.randint(1, max_days)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    result = datetime.datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second)
    return result


def _generate_random_datetimes(number_of_elements: int) -> list[datetime.datetime]:
    """Generate random datetimes for testing."""
    random_datetimes = [_generate_random_datetime() for _ in range(number_of_elements)]
    return random_datetimes


@pytest.fixture(scope="session")
def unordered_output_file_content(tmp_path_factory: pytest.TempPathFactory) -> list[list[str]]:
    """Generate file content equivalent to an unordered output file."""
    example_output_file_path = (
        pathlib.Path(__file__).parent
        / "examples"
        / "example_0"
        / "expected_output"
        / "blobs_11ec8933-1456-4942-922b-94e5878bb991.tsv"
    )

    with open(file=example_output_file_path, mode="r") as io:
        example_output_lines = io.readlines()
    base_line = example_output_lines[1].split("\t")

    random_datetimes = _generate_random_datetimes(number_of_elements=100)

    scrambled_example_output_lines = [
        [base_line[0], random_datetime, base_line[2], base_line[3]] for random_datetime in random_datetimes
    ]
    return scrambled_example_output_lines


def test_output_file_reordering() -> None:
    """
    Performing parallelized parsing can result in both race conditions and a break to chronological ordering.

    This is a test for the utility function for reordering the output of a parsed log file according to time.
    """
    pass
