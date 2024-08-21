# import pathlib
#
# import py
#

# def test_bin_reduced_s3_logs_by_object_key_example_0(tmpdir: py.path.local) -> None:
#     tmpdir = pathlib.Path(tmpdir)
#
#     file_parent = pathlib.Path(__file__).parent
#     example_folder_path = file_parent / "examples" / "binning_example_0"
#     reduced_s3_logs_folder_path = example_folder_path / "reduced_logs"
#
#     test_binned_s3_logs_folder_path = tmpdir / "binned_example_0"
#     test_binned_s3_logs_folder_path.mkdir(exist_ok=True)
#
#     expected_binned_s3_logs_folder_path = example_folder_path / "expected_output"
#     expected_binned_s3_log_file_path = expected_reduced_s3_logs_folder_path / "2020" / "01" / "01.tsv"
#
#     dandi_s3_log_parser.bin_all_reduced_s3_logs_by_object_key(
#         reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
#         binned_s3_logs_folder_path=test_binned_s3_logs_folder_path,
#     )
#
#     test_reduced_s3_log = pandas.read_table(filepath_or_buffer=test_reduced_s3_log_file_path)
#     expected_reduced_s3_log = pandas.read_table(filepath_or_buffer=expected_reduced_s3_log_file_path)
#
#     pandas.testing.assert_frame_equal(left=test_reduced_s3_log, right=expected_reduced_s3_log)
