import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_reduce_dandi_raw_s3_log_example_0(tmpdir: py.path.local) -> None:
    """
    Most basic test of functionality.

    If there are failures in the parsing of any lines found in application,
    please raise an issue and contribute them to the example log collection.
    """
    tmpdir = pathlib.Path(tmpdir)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "reduced_example_0"
    example_raw_s3_log_file_path = examples_folder_path / "0.log"
    expected_reduced_s3_logs_folder_path = examples_folder_path / "expected_output"

    test_reduced_s3_logs_folder_path = tmpdir / "reduced_example_0"
    test_reduced_s3_logs_folder_path.mkdir(exist_ok=True)

    dandi_s3_log_parser.reduce_dandi_raw_s3_log(
        raw_s3_log_file_path=example_raw_s3_log_file_path,
        reduced_s3_logs_folder_path=test_reduced_s3_logs_folder_path,
    )
    test_output_file_paths = list(test_reduced_s3_logs_folder_path.rglob("*.tsv"))

    number_of_output_files = len(test_output_file_paths)
    assert number_of_output_files != 0, f"Test expected_output folder ({test_reduced_s3_logs_folder_path}) is empty!"

    # Increment this over time as more examples are added
    expected_number_of_output_files = 2
    assert (
        number_of_output_files == expected_number_of_output_files
    ), f"The number of asset files ({number_of_output_files}) does not match expectation!"

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
