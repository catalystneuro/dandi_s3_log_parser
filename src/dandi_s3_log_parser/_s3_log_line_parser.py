"""
Primary functions for parsing a single line of a raw S3 log.

The strategy is to...

1) Parse the raw line into a list of strings using a combination of regex patterns and custom string manipulation.
2) Construct a FullLogLine object from the parsed line. A collections.namedtuple object is used for performance.
3) Reduce and map the information from the FullLogLine into a collections.defaultdict object.
   Some of the mapping operations at this step include...
      - Handling the timestamp in memory as a datetime.datetime object.
      - Filtering out log lines from excluded IPs.
"""

from ._globals import (
    _S3_LOG_REGEX,
    _FullLogLine,
)


def _parse_s3_log_line(*, raw_s3_log_line: str) -> list[str]:
    """
    The current method of parsing lines of an S3 log file.

    Bad lines reported in https://github.com/catalystneuro/dandi_s3_log_parser/issues/18 led to quote scrubbing
    as a pre-step. No self-contained single regex was found that could account for this uncorrected strings.
    """
    parsed_log_line = [a or b or c for a, b, c in _S3_LOG_REGEX.findall(string=raw_s3_log_line)]

    number_of_parsed_items = len(parsed_log_line)

    # Everything worked as expected
    if number_of_parsed_items <= 26:
        return parsed_log_line

    potentially_cleaned_raw_line = _attempt_to_remove_quotes(
        raw_s3_log_line=raw_s3_log_line, bad_parsed_line=parsed_log_line
    )
    parsed_log_line = [a or b or c for a, b, c in _S3_LOG_REGEX.findall(string=potentially_cleaned_raw_line)]

    return parsed_log_line


def _attempt_to_remove_quotes(*, raw_s3_log_line: str, bad_parsed_line: str) -> str:
    """
    Attempt to remove bad quotes from a raw line of an S3 log file.

    These quotes are not properly escaped and are causing issues with the regex pattern.
    Various attempts to fix the regex failed, so this is the most reliable correction I could find.
    """
    starting_quotes_indices = _find_all_possible_substring_indices(string=raw_s3_log_line, substring=' "')
    ending_quotes_indices = _find_all_possible_substring_indices(string=raw_s3_log_line, substring='" ')

    # If even further unexpected structure, just return the bad parsed line so that the error reporter can catch it
    if len(starting_quotes_indices) == 0:  # pragma: no cover
        return bad_parsed_line
    if len(starting_quotes_indices) != len(ending_quotes_indices):  # pragma: no cover
        return bad_parsed_line

    cleaned_raw_s3_log_line = raw_s3_log_line[0 : starting_quotes_indices[0]]
    for counter in range(1, len(starting_quotes_indices) - 1):
        next_block = raw_s3_log_line[ending_quotes_indices[counter - 1] + 2 : starting_quotes_indices[counter]]
        cleaned_raw_s3_log_line += " - " + next_block
    cleaned_raw_s3_log_line += " - " + raw_s3_log_line[ending_quotes_indices[-1] + 2 :]

    return cleaned_raw_s3_log_line


def _find_all_possible_substring_indices(*, string: str, substring: str) -> list[int]:
    indices = list()
    start = 0
    max_iter = 10**6
    while True and start < max_iter:
        next_index = string.find(substring, start)
        if next_index == -1:  # .find(...) was unable to locate the substring
            break
        indices.append(next_index)
        start = next_index + 1

    if start >= max_iter:
        message = (
            f"Exceeded maximum iterations in `_find_all_possible_substring_indices` on `{string=}` with `{substring=}`."
        )
        raise StopIteration(message)

    return indices


def _get_full_log_line(
    *,
    parsed_s3_log_line: list[str],
) -> _FullLogLine:
    number_of_parsed_items = len(parsed_s3_log_line)
    match number_of_parsed_items:
        # Seen in a few good lines; don't know why some fields are not detected
        case 24:
            parsed_s3_log_line.append("-")
            parsed_s3_log_line.append("-")
            return _FullLogLine(*parsed_s3_log_line)
        # Expected length for most good lines, don't know why they don't include the extra piece on the end
        case 25:
            parsed_s3_log_line.append("-")
            return _FullLogLine(*parsed_s3_log_line)
        case 26:
            return _FullLogLine(*parsed_s3_log_line)
        case _:
            raise ValueError(
                f"Unexpected number of parsed items: {number_of_parsed_items}. Parsed line: {parsed_s3_log_line}"
            )
