import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_order_and_anonymize(tmpdir: py.path.local) -> None:
    tmpdir = pathlib.Path(tmpdir)

    unordered_example_base_folder_path = pathlib.Path(__file__).parent / "examples" / "order_and_anonymize_example_0"
    unordered_parsed_s3_log_folder_path = unordered_example_base_folder_path / "unordered_parsed_logs"
    ordered_and_anonymized_s3_log_folder_path = tmpdir

    dandi_s3_log_parser.order_and_anonymize_parsed_logs(
        unordered_parsed_s3_log_folder_path=unordered_parsed_s3_log_folder_path,
        ordered_and_anonymized_s3_log_folder_path=ordered_and_anonymized_s3_log_folder_path,
    )

    parsed_log_file_stems = [path.name for path in unordered_parsed_s3_log_folder_path.iterdir()]
    expected_output_folder_path = unordered_example_base_folder_path / "expected_output"
    for parsed_log_file_name in parsed_log_file_stems:
        test_ordered_and_anonymized_s3_log_file_path = ordered_and_anonymized_s3_log_folder_path / parsed_log_file_name
        expected_ordered_and_anonymized_s3_log_file_path = expected_output_folder_path / parsed_log_file_name

        test_ordered_and_anonymized_s3_log = pandas.read_table(
            filepath_or_buffer=test_ordered_and_anonymized_s3_log_file_path, index_col=0,
        )
        expected_ordered_and_anonymized_s3_log = pandas.read_table(
            filepath_or_buffer=expected_ordered_and_anonymized_s3_log_file_path, index_col=0,
        )
        pandas.testing.assert_frame_equal(
            left=test_ordered_and_anonymized_s3_log, right=expected_ordered_and_anonymized_s3_log,
        )
