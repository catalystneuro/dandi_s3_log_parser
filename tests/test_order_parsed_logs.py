import pathlib
import py
import pandas

import dandi_s3_log_parser


def test_output_file_reordering(tmpdir: py.path.local) -> None:
    """
    Performing parallelized parsing can result in both race conditions and a break to chronological ordering.

    This is a test for the utility function for reordering the output of a parsed log file according to time.
    """
    tmpdir = pathlib.Path(tmpdir)

    unordered_example_base_folder_path = pathlib.Path(__file__).parent / "examples" / "unordered_example_0"
    unordered_parsed_s3_log_folder_path = unordered_example_base_folder_path / "unordered_parsed_logs"
    ordered_parsed_s3_log_folder_path = tmpdir

    dandi_s3_log_parser.order_parsed_logs(
        unordered_parsed_s3_log_folder_path=unordered_parsed_s3_log_folder_path,
        ordered_parsed_s3_log_folder_path=ordered_parsed_s3_log_folder_path,
    )

    parsed_log_file_stems = [path.name for path in unordered_parsed_s3_log_folder_path.iterdir()]
    expected_output_folder_path = unordered_example_base_folder_path / "expected_output"
    for parsed_log_file_name in parsed_log_file_stems:
        test_parsed_s3_log_file_path = ordered_parsed_s3_log_folder_path / parsed_log_file_name
        expected_parsed_s3_log_file_path = expected_output_folder_path / parsed_log_file_name

        test_parsed_s3_log = pandas.read_table(filepath_or_buffer=test_parsed_s3_log_file_path, index_col=0)
        expected_parsed_s3_log = pandas.read_table(filepath_or_buffer=expected_parsed_s3_log_file_path, index_col=0)
        pandas.testing.assert_frame_equal(left=test_parsed_s3_log, right=expected_parsed_s3_log)
