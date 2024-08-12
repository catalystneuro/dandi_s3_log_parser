import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_parse_dandi_raw_s3_log_bad_lines(tmpdir: py.path.local):
    """'ordered_example_2' contains the basic test cases as well as a collection of 'bad lines' contributed over time."""
    tmpdir = pathlib.Path(tmpdir)

    # Count initial error folder contents
    error_folder = dandi_s3_log_parser.DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    initial_number_of_error_folder_contents = len(error_folder_contents)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "ordered_example_2"
    example_raw_s3_log_file_path = examples_folder_path / "example_dandi_s3_log.log"
    expected_parsed_s3_log_folder_path = examples_folder_path / "expected_output"

    test_parsed_s3_log_folder_path = tmpdir / "parsed_example_2"
    dandi_s3_log_parser.parse_dandi_raw_s3_log(
        raw_s3_log_file_path=example_raw_s3_log_file_path,
        parsed_s3_log_folder_path=test_parsed_s3_log_folder_path,
    )
    test_output_file_paths = [path for path in test_parsed_s3_log_folder_path.iterdir() if path.is_file()]

    number_of_output_files = len(test_output_file_paths)
    expected_number_of_output_files = 3
    assert number_of_output_files == expected_number_of_output_files

    expected_asset_ids = [path.stem for path in expected_parsed_s3_log_folder_path.iterdir() if path.is_file()]
    for test_parsed_s3_log_file_path in test_output_file_paths:
        assert (
            test_parsed_s3_log_file_path.stem in expected_asset_ids
        ), f"Asset ID {test_parsed_s3_log_file_path.stem} not found in expected asset IDs!"

        test_parsed_s3_log = pandas.read_table(filepath_or_buffer=test_parsed_s3_log_file_path, index_col=0)
        expected_parsed_s3_log_file_path = (
            expected_parsed_s3_log_folder_path / f"{test_parsed_s3_log_file_path.stem}.tsv"
        )
        expected_parsed_s3_log = pandas.read_table(filepath_or_buffer=expected_parsed_s3_log_file_path, index_col=0)
        pandas.testing.assert_frame_equal(left=test_parsed_s3_log, right=expected_parsed_s3_log)

    post_test_error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    assert (
        len(post_test_error_folder_contents) == initial_number_of_error_folder_contents
    ), "Errors occurred during line parsing!"
