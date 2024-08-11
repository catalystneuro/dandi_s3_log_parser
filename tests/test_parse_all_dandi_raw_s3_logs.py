import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_parse_all_dandi_raw_s3_logs_example_0(tmpdir: py.path.local):
    tmpdir = pathlib.Path(tmpdir)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "ordered_example_1"
    expected_parsed_s3_log_folder_path = examples_folder_path / "expected_output"

    test_parsed_s3_log_folder_path = tmpdir / "parsed_example_1"
    dandi_s3_log_parser.parse_all_dandi_raw_s3_logs(
        base_raw_s3_log_folder_path=examples_folder_path,
        parsed_s3_log_folder_path=test_parsed_s3_log_folder_path,
    )
    test_output_file_paths = list(test_parsed_s3_log_folder_path.iterdir())

    number_of_output_files = len(test_output_file_paths)
    assert number_of_output_files != 0, f"Test expected_output folder ({test_parsed_s3_log_folder_path}) is empty!"

    # Increment this over time as more examples are added
    expected_number_of_output_files = 2
    assert (
        number_of_output_files == expected_number_of_output_files
    ), f"The number of asset files ({number_of_output_files}) does not match expectation!"

    expected_asset_ids = [file_path.stem for file_path in expected_parsed_s3_log_folder_path.iterdir()]
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


# TODO: add CLI
