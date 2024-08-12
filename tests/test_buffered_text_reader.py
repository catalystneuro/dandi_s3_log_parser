import pathlib
import sys

import pytest

import dandi_s3_log_parser


@pytest.fixture(scope="session")
def large_text_file_path(tmp_path_factory: pytest.TempPathFactory):
    tmp_path = tmp_path_factory.mktemp("large_text_file")

    # Generate a test file ~10 MB in total size
    # Content does not matter, each line is ~100 bytes
    test_file_path = tmp_path / "large_text_file.txt"
    fill_string = "a" * 60 + "\n"
    content = [fill_string for _ in range(10**5)]
    with open(file=test_file_path, mode="w") as test_file:
        test_file.writelines(content)

    return test_file_path


@pytest.fixture(scope="session")
def single_line_text_file_path(tmp_path_factory: pytest.TempPathFactory):
    """For testing the ValueError case during iteration."""
    tmp_path = tmp_path_factory.mktemp("single_line_text_file")

    # Generate test file ~3 MB in total size, consisting of only a single line
    test_file_path = tmp_path / "single_line_text_file.txt"
    with open(file=test_file_path, mode="w") as test_file:
        test_file.write("a" * 30**6)

    return test_file_path


def test_buffered_text_reader(large_text_file_path: pathlib.Path):
    maximum_buffer_size_in_bytes = 10**6  # 1 MB
    buffered_text_reader = dandi_s3_log_parser.BufferedTextReader(
        file_path=large_text_file_path,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
    )

    assert iter(buffered_text_reader) is buffered_text_reader, "BufferedTextReader object is not iterable!"

    for buffer_index, buffer in enumerate(buffered_text_reader):
        assert isinstance(buffer, list), "BufferedTextReader object did not load a buffer as a list!"
        assert (
            sys.getsizeof(buffer) <= buffered_text_reader.buffer_size_in_bytes
        ), "BufferedTextReader object loaded a buffer exceeding the threshold!"

    assert buffer_index == 18, "BufferedTextReader object did not load the correct number of buffers!"

    with pytest.raises(StopIteration):
        next(buffered_text_reader)


def test_value_error(single_line_text_file_path: pathlib.Path):
    maximum_buffer_size_in_bytes = 10**6  # 1 MB
    with pytest.raises(ValueError) as error_info:
        buffered_text_reader = dandi_s3_log_parser.BufferedTextReader(
            file_path=single_line_text_file_path,
            maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        )
        next(buffered_text_reader)

    expected_message = (
        "BufferedTextReader encountered a line at offset 0 that exceeds the buffer size! "
        "Try increasing the `maximum_buffer_size_in_bytes` to account for this line."
    )
    assert str(error_info.value) == expected_message
