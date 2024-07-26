# Raw S3 Log parser

Simple reductions of consolidated S3 logs (consolidation step not included in this repository) into minimal information for sharing and plotting.

Developed for the DANDI Archive.



# Usage

To iteratively parse all historical logs all at once:

```bash
parse_all_dandi_raw_s3_logs --base_raw_s3_log_folder_path < base log folder > --parsed_s3_log_folder_path < output folder > --excluded_ips < comma-separated list of known IPs to exclude >
```

For example, on Drogon:

```bash
parse_all_dandi_raw_s3_logs --base_raw_s3_log_folder_path /mnt/backup/dandi/dandiarchive-logs --parsed_s3_log_folder_path /mnt/backup/dandi/dandiarchive-logs-cody/parsed_7_13_2024/GET_per_asset_id --excluded_ips < Drogon's IP >
```

To parse only a single log file at a time, such as in a CRON job:

```bash
parse_dandi_raw_s3_log --raw_s3_log_file_path < s3 log file path > --parsed_s3_log_folder_path < output folder > --excluded_ips < comma-separated list of known IPs to exclude >
```



# Submit line decoding errors

Please post line decoding errors collected from your local config file as issues before submitting PRs contributing them as examples, to more easily correct anonymization aspects.



# Developer notes

`.log` file suffixes should typically be ignored when working with Git, so when committing changes to the example log collection, you will have to forcibly include it with

```bash
git add -f <example file name>.log
```
