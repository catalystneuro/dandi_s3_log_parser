# Contributing

If you would like to contribute some failing examples to this test suite, please contact the maintainer (cody.c.baker.phd@gmail.com) prior to opening a pull request.

There are multiple fields that may require anonymization prior to being shared openly.

The names of the following test files are patterned off of the S3 log filename convention and may not accurately respect the timestamps of the lines within.



# 2020-01-01-05-06-35-0123456789ABCDEF (Easy lines)

The 'easy' collection contains the most typical lines which follow a nice, simple, and reliable structure.



# 2021-01-01-05-06-35-0123456789ABCDEF (Multiple requesters)

The 'multiple requesters' collection repeats access to a handful of assets from several different requesters, so that the summaries accumulate more than one request per asset, per day, and per region.

Every requester is a documentation-range address (RFC 5737), which a real geolocation resolves to `bogon` rather than to any place. `mocked_ips/ip_to_region.yaml`, loaded into a `MappingRegionResolver` by the `mocked_region_resolver` fixture, stands in for a geolocation of them, mapping all but one to an invented region. It is spread over enough distinct regions for a `by_region.tsv` to clear its disclosure threshold and be published, and the requester left out of it is summarized under the unresolved `missing` label.

Each asset of this collection is accessed only from this file. An asset whose requests are split across log files is extracted in an order that depends on how the files are batched across workers, which makes the expected output unstable between serial and parallel runs.



# 2022-01-01-05-06-35-0123456789ABCDEF (Hard lines)

The 'hard' collection contains many of the most difficult lines to extract as they were found from error reports.
