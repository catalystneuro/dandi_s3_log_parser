import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_reduce_raw_s3_log_example_bad_lines_fast_case(tmpdir: py.path.local) -> None:
    tmpdir = pathlib.Path(tmpdir)

    # Count initial error folder contents
    error_folder = dandi_s3_log_parser.DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    initial_number_of_error_folder_contents = len(error_folder_contents)

    file_parent = pathlib.Path(__file__).parent
    example_folder_path = file_parent / "examples" / "reduction_example_2"
    example_raw_s3_log_file_path = example_folder_path / "raw_logs" / "2022" / "04" / "06.log"

    test_reduced_s3_logs_folder_path = tmpdir / "reduced_example_bad_lines_fast_case"
    test_reduced_s3_log_file_path = test_reduced_s3_logs_folder_path / "2022" / "04" / "06.tsv"
    test_reduced_s3_log_file_path.parent.mkdir(parents=True, exist_ok=True)

    expected_reduced_s3_logs_folder_path = example_folder_path / "expected_output"
    expected_reduced_s3_log_file_path = expected_reduced_s3_logs_folder_path / "2022" / "04" / "06.tsv"

    dandi_s3_log_parser.reduce_raw_s3_log(
        raw_s3_log_file_path=example_raw_s3_log_file_path,
        reduced_s3_log_file_path=test_reduced_s3_log_file_path,
        # The two specifications below trigger the 'fast' parsing
        fields_to_reduce=["object_key", "timestamp", "bytes_sent", "ip_address"],
        object_key_parents_to_reduce=["blobs", "zarr"],
    )

    test_reduced_s3_log = pandas.read_table(filepath_or_buffer=test_reduced_s3_log_file_path)
    expected_reduced_s3_log = pandas.read_table(filepath_or_buffer=expected_reduced_s3_log_file_path)

    pandas.testing.assert_frame_equal(left=test_reduced_s3_log, right=expected_reduced_s3_log)

    post_test_error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    assert (
        len(post_test_error_folder_contents) == initial_number_of_error_folder_contents
    ), "Errors occurred during line parsing!"


def test_reduce_raw_s3_log_example_bad_lines_basic_case(tmpdir: py.path.local) -> None:
    tmpdir = pathlib.Path(tmpdir)

    # Count initial error folder contents
    error_folder = dandi_s3_log_parser.DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "errors"
    error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    initial_number_of_error_folder_contents = len(error_folder_contents)

    file_parent = pathlib.Path(__file__).parent
    example_folder_path = file_parent / "examples" / "reduction_example_2"
    example_raw_s3_log_file_path = example_folder_path / "raw_logs" / "2022" / "04" / "06.log"

    test_reduced_s3_logs_folder_path = tmpdir / "reduced_example_bad_lines_basic_case"
    test_reduced_s3_log_file_path = test_reduced_s3_logs_folder_path / "2022" / "04" / "06.tsv"
    test_reduced_s3_log_file_path.parent.mkdir(parents=True, exist_ok=True)

    expected_reduced_s3_logs_folder_path = example_folder_path / "expected_output"
    expected_reduced_s3_log_file_path = expected_reduced_s3_logs_folder_path / "2022" / "04" / "06.tsv"

    object_key_handler = dandi_s3_log_parser._dandi_s3_log_file_reducer._get_default_dandi_object_key_handler()
    dandi_s3_log_parser.reduce_raw_s3_log(
        raw_s3_log_file_path=example_raw_s3_log_file_path,
        reduced_s3_log_file_path=test_reduced_s3_log_file_path,
        fields_to_reduce=["object_key", "timestamp", "bytes_sent", "ip_address"],
        object_key_handler=object_key_handler,
    )

    test_reduced_s3_log = pandas.read_table(filepath_or_buffer=test_reduced_s3_log_file_path)
    expected_reduced_s3_log = pandas.read_table(filepath_or_buffer=expected_reduced_s3_log_file_path)

    pandas.testing.assert_frame_equal(left=test_reduced_s3_log, right=expected_reduced_s3_log)

    post_test_error_folder_contents = list(error_folder.iterdir()) if error_folder.exists() else list()
    assert (
        len(post_test_error_folder_contents) == initial_number_of_error_folder_contents
    ), "Errors occurred during line parsing!"
