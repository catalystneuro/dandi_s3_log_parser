import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_reduce_all_dandi_raw_s3_logs_example_1(tmpdir: py.path.local) -> None:
    """Basic test for parsing of all DANDI raw S3 logs in a directory."""
    tmpdir = pathlib.Path(tmpdir)

    file_parent = pathlib.Path(__file__).parent
    example_folder_path = file_parent / "examples" / "reduction_example_1"
    example_raw_s3_logs_folder_path = example_folder_path / "raw_logs"

    test_reduced_s3_logs_folder_path = tmpdir / "reduction_example_1"
    test_reduced_s3_logs_folder_path.mkdir(exist_ok=True)

    expected_reduced_s3_logs_folder_path = example_folder_path / "expected_output"

    dandi_s3_log_parser.reduce_all_dandi_raw_s3_logs(
        raw_s3_logs_folder_path=example_raw_s3_logs_folder_path,
        reduced_s3_logs_folder_path=test_reduced_s3_logs_folder_path,
        maximum_number_of_workers=2,
    )
    test_output_file_paths = list(test_reduced_s3_logs_folder_path.rglob("*.tsv"))

    test_number_of_output_files = len(test_output_file_paths)
    expected_number_of_output_files = 2
    assert (
        test_number_of_output_files == expected_number_of_output_files
    ), f"The number of asset files ({test_number_of_output_files}) does not match expectation!"

    # First file
    test_reduced_s3_log_file_path = test_reduced_s3_logs_folder_path / "2020" / "01" / "01.tsv"
    expected_reduced_s3_log_file_path = expected_reduced_s3_logs_folder_path / "2020" / "01" / "01.tsv"

    test_reduced_s3_log = pandas.read_table(filepath_or_buffer=test_reduced_s3_log_file_path)
    expected_reduced_s3_log = pandas.read_table(filepath_or_buffer=expected_reduced_s3_log_file_path)

    pandas.testing.assert_frame_equal(left=test_reduced_s3_log, right=expected_reduced_s3_log)

    # Second file
    test_reduced_s3_log_file_path = test_reduced_s3_logs_folder_path / "2021" / "02" / "03.tsv"
    expected_reduced_s3_log_file_path = expected_reduced_s3_logs_folder_path / "2021" / "02" / "03.tsv"

    test_reduced_s3_log = pandas.read_table(filepath_or_buffer=test_reduced_s3_log_file_path)
    expected_reduced_s3_log = pandas.read_table(filepath_or_buffer=expected_reduced_s3_log_file_path)

    pandas.testing.assert_frame_equal(left=test_reduced_s3_log, right=expected_reduced_s3_log)


# TODO: add CLI
