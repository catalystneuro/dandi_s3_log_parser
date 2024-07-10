"""Call the raw S3 log parser from the command line."""

from ._s3_log_file_parser import parse_raw_s3_log


# TODO
def main() -> None:
    parse_raw_s3_log()


if __name__ == "__main__":
    main()
