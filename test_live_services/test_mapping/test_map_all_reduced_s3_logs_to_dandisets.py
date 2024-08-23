import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_map_all_reduced_s3_logs_to_dandisets(tmpdir: py.path.local):
    tmpdir = pathlib.Path(tmpdir)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "mapped_to_dandisets_example_0"
    example_binned_s3_logs_folder_path = examples_folder_path / "binned_logs"

    test_mapped_s3_logs_folder_path = tmpdir

    expected_output_folder_path = examples_folder_path / "expected_output"

    dandi_s3_log_parser.map_binned_s3_logs_to_dandisets(
        binned_s3_logs_folder_path=example_binned_s3_logs_folder_path,
        mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path,
        object_type="blobs",
    )
    dandi_s3_log_parser.map_binned_s3_logs_to_dandisets(
        binned_s3_logs_folder_path=example_binned_s3_logs_folder_path,
        mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path,
        object_type="zarr",
    )

    test_file_paths = {
        path.relative_to(test_mapped_s3_logs_folder_path): path
        for path in test_mapped_s3_logs_folder_path.rglob("*.tsv")
    }
    expected_file_paths = {
        path.relative_to(expected_output_folder_path): path for path in expected_output_folder_path.rglob("*.tsv")
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_output_folder_path)
        test_file_path = test_mapped_s3_logs_folder_path / relative_file_path

        # Pandas assertion makes no reference to the file being tested when it fails
        print(f"{test_file_path=}")
        print(f"{expected_file_path=}")

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)

        pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
