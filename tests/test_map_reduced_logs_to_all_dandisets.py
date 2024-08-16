import pathlib

import pandas
import py

import dandi_s3_log_parser


def test_map_reduced_logs_to_dandisets(tmpdir: py.path.local):
    tmpdir = pathlib.Path(tmpdir)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "mapped_to_dandiset_example_0"
    reduced_s3_logs_folder_path = examples_folder_path / "reduced_logs"
    dandiset_logs_folder_path = tmpdir

    dandi_s3_log_parser.map_reduced_logs_to_dandisets(
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        dandiset_logs_folder_path=dandiset_logs_folder_path,
    )

    expected_output_folder_path = examples_folder_path / "expected_output"

    # Ensure to extra folders were created
    test_dandiset_id_folder_paths = [
        dandiset_id_folder_path.stem for dandiset_id_folder_path in dandiset_logs_folder_path.iterdir()
    ]
    expected_dandiset_id_folder_paths = [
        dandiset_id_folder_path.stem for dandiset_id_folder_path in expected_output_folder_path.iterdir()
    ]
    assert set(test_dandiset_id_folder_paths) == set(expected_dandiset_id_folder_paths)

    test_dandiset_version_id_file_paths = {
        f"{version_id_file_path.parent.name}/{version_id_file_path.name}": version_id_file_path
        for dandiset_id_folder_path in dandiset_logs_folder_path.iterdir()
        for version_id_file_path in dandiset_id_folder_path.iterdir()
    }
    expected_dandiset_version_id_file_paths = {
        f"{version_id_file_path.parent.name}/{version_id_file_path.name}": version_id_file_path
        for dandiset_id_folder_path in expected_output_folder_path.iterdir()
        for version_id_file_path in dandiset_id_folder_path.iterdir()
    }
    assert set(test_dandiset_version_id_file_paths.keys()) == set(expected_dandiset_version_id_file_paths.keys())

    for expected_version_id_file_path in expected_dandiset_version_id_file_paths.values():
        test_version_id_file_path = (
            dandiset_logs_folder_path / expected_version_id_file_path.parent.name / expected_version_id_file_path.name
        )

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_version_id_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_version_id_file_path, index_col=0)

        pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
