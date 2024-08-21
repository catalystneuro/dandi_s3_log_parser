import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_reduce_dandi_raw_s3_log_bad_lines(tmpdir: py.path.local) -> None:
    """
    'parsed_example_2' contains the basic test cases as well as a collection of 'bad lines' contributed over time.
    """
    tmpdir = pathlib.Path(tmpdir)

    # Count initial error folder contents
    error_folder = dandi_s3_log_parser.DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    initial_number_of_error_folder_contents = len(error_folder_contents)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "reduced_example_2"
    example_raw_s3_log_file_path = examples_folder_path / "0.log"
    expected_reduced_s3_logs_folder_path = examples_folder_path / "expected_output"

    test_reduced_s3_logs_folder_path = tmpdir / "reduced_example_2"
    test_reduced_s3_logs_folder_path.mkdir(exist_ok=True)

    dandi_s3_log_parser.reduce_dandi_raw_s3_log(
        raw_s3_log_file_path=example_raw_s3_log_file_path,
        reduced_s3_logs_folder_path=test_reduced_s3_logs_folder_path,
    )
    test_output_file_paths = list(test_reduced_s3_logs_folder_path.rglob("*.tsv"))

    number_of_output_files = len(test_output_file_paths)
    expected_number_of_output_files = 3
    assert number_of_output_files == expected_number_of_output_files

    expected_asset_ids = [path.stem for path in expected_reduced_s3_logs_folder_path.rglob("*.tsv")]
    for test_parsed_s3_log_file_path in test_output_file_paths:
        assert (
            test_parsed_s3_log_file_path.stem in expected_asset_ids
        ), f"Asset ID {test_parsed_s3_log_file_path.stem} not found in expected asset IDs!"

        test_parsed_s3_log = pandas.read_table(filepath_or_buffer=test_parsed_s3_log_file_path)

        blob_id = test_parsed_s3_log_file_path.stem
        expected_parsed_s3_log_file_path = (
            expected_reduced_s3_logs_folder_path / "blobs" / blob_id[:3] / blob_id[3:6] / f"{blob_id}.tsv"
        )
        expected_parsed_s3_log = pandas.read_table(filepath_or_buffer=expected_parsed_s3_log_file_path)

        pandas.testing.assert_frame_equal(left=test_parsed_s3_log, right=expected_parsed_s3_log)

    post_test_error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    assert (
        len(post_test_error_folder_contents) == initial_number_of_error_folder_contents
    ), "Errors occurred during line parsing!"
