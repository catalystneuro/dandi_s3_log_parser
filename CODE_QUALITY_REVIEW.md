# Code Quality Review

Functionality-preserving review of `s3-log-extraction` at commit `c44178a`, covering all 40 modules under
`src/s3_log_extraction/` (about 4,500 lines), the six `.awk` scripts, `tools/`, `tests/` (about 3,500 lines),
and the manifest and CI configuration.

No changes have been applied. Every finding below was traced to its call sites before being recorded.

## Summary

This codebase is in good health. It is not a codebase that needs rescuing, and most of what follows is
consolidation rather than repair. Several things stand out as genuinely well built. Docstrings are thorough and
explain intent rather than restating signatures, and the privacy reasoning in `summarize/globals.py` and the
label taxonomy in `ip_utils/_resolver.py` are documented at a level that is rare. The recent reworks recorded in
`CHANGELOG.md` left behind remarkably little debris. The prefix-table indexing in `_build_prefix_tables` is a
real piece of engineering, and the pickling contract on `IpRegionResolver` is both correct and explained. Ruff
passes cleanly under the project's own configuration, and there is not a single unused import anywhere in the
tree.

The dominant theme is **duplication by copy-paste across sibling modules**. It appears in four places, and in
each the copies have drifted slightly, which is exactly what makes the duplication expensive. The five
pre-validators in `validate/` share a `__hash__` method that is byte-for-byte identical five times over and a
`_run_validation` that differs only in one error string and one `env` argument. `RemoteS3LogAccessExtractor` and
`S3LogAccessExtractor` are siblings rather than parent and child, and they carry an identical `_run_extraction`,
an identical child-process copy-back loop, and an identical record-corruption message. The four modules in
`summarize/` repeat the same four-line activity-column coercion loop verbatim. And `utils/inventory.py` walks
the same S3 inventory in two functions with about fifteen identical lines each time. The code itself
acknowledges this: `_generate_archive_summaries.py:50` carries a `# TODO: deduplicate code into common helpers
across tools`.

The second theme is **a pervasive low-grade idiom drift** that a linter would catch if it were asked to. There
are 21 instances of assigning a value to a local and immediately returning it, four unnecessary set
comprehensions, three unnecessary `pass` statements, and three uses of `any()` as a non-emptiness test. None of
these matter alone. Together they are about 40 lines of noise spread across 12 files, and they are all
mechanically verifiable.

The third theme is **drift from the project's own AGENTS.md rules**. Two function-local imports in
`utils/inventory.py` are not there to break a cycle, and I confirmed by experiment that they import fine at
module top. Three test modules import from private modules when the names they want are already re-exported
publicly. `beartype` is a declared hard dependency applied to exactly two of the package's public functions.

Finally, there are three pieces of **provably inert configuration**: ten of the eleven `ruff` ignore entries name
rules that the `select = ["F", "E", "I"]` set never reaches, and the `hatch-vcs` version source is never consulted
because `[project]` declares a static `version` without listing it in `dynamic`. Both were verified empirically
rather than by reading the documentation.

Two items deserve attention beyond code quality, and both are recorded under **Needs Verification** rather than
proposed as changes, because fixing either would alter behavior: the `update summaries` command accepts three
options it silently ignores, and `_handle_aws_credentials` has an operator-precedence defect that makes one of
its two guards unreachable.

### Findings by category

| Category | High confidence | Medium | Low | Total |
| --- | --- | --- | --- | --- |
| Dead code | 5 | 1 | 0 | 6 |
| Duplication | 9 | 6 | 0 | 15 |
| Consolidation | 3 | 4 | 0 | 7 |
| Cleanup | 12 | 5 | 1 | 18 |
| **Total** | **29** | **16** | **1** | **46** |

Plus 12 items under **Needs Verification**, which are observations rather than proposed changes.

All confidence ratings are about *safety* (whether the change preserves behavior), not about whether the finding
is real.

---

## Findings

### Dead Code

#### DC-1 — `_ip_in_cidr` is unreferenced, and takes `import warnings` with it

- **Location**: `src/s3_log_extraction/ip_utils/_ip_utils.py` (lines 41-54, `_ip_in_cidr`; plus `import warnings` at line 4)
- **Issue**: The function has no callers anywhere. It was superseded in v1.11.2 by the prefix-table matching in
  `_resolver._build_prefix_tables`, which does its own `ipaddress.ip_network` parsing and emits its own
  "Skipping invalid CIDR entry" warning at `_resolver.py:81-84`. The stale copy invites someone to reach for the
  linear per-address CIDR scan the rework deliberately removed.
- **Proposed change**: Delete `_ip_in_cidr` (lines 41-54) and the now-unused `import warnings` at line 4. No
  other symbol in the module uses `warnings`.
- **Confidence it's safe**: **High**. A repo-wide search over `*.py`, `*.awk`, `*.md`, `*.toml`, and `*.yml`
  returns exactly one hit, the definition itself. The name is in no `__all__`, is not referenced from
  `_hidden_top_level_imports.py`, is not monkeypatched by any test (contrast `_request_cidr_range`, which *is*
  patched by name at `tests/test_ip_utils.py:274`), and appears in no awk script or workflow.
- **Verification**: `grep -rn "_ip_in_cidr\|warnings" src/s3_log_extraction/ip_utils/_ip_utils.py` should return
  nothing after the change, then `python -m pytest tests/ -m "not remote" -q`.

#### DC-2 — Three unreachable `None` guards in the remote URL scan

- **Location**: `src/s3_log_extraction/extractors/_remote_s3_log_access_extractor.py` (lines 297-298, 307-308, 331-332, inside `_get_unprocessed_s3_urls_from_remote`)
- **Issue**: Each guard reads `if months_result is None: continue` (and the `days_result` / `s3_urls_result`
  equivalents). `_deploy_subprocess` returns `None` on exactly one path, `extractors/_utils.py:90-91`, which
  requires `ignore_errors=True`. None of the four `s5cmd ls` call sites in this function passes
  `ignore_errors`, so every call either raises `RuntimeError` or returns `result.stdout`, which is always a
  `str`. The guards can never fire. That the first call at line 286 has no such guard, and dereferences
  `years_result.splitlines()` directly, shows the inconsistency.
- **Proposed change**: Remove the three `if ... is None: continue` guards. Alternatively, keep them and pass
  `ignore_errors=True` at those three call sites, but that is a behavior change and is out of scope here.
- **Confidence it's safe**: **High**. `_deploy_subprocess` has exactly two `return` statements
  (`_utils.py:91` and `_utils.py:93`) and one `raise`; the `None` return is guarded by
  `ignore_errors is True`. Default is `False` and no caller in this function overrides it.
- **Verification**: `grep -n "ignore_errors" src/s3_log_extraction/extractors/` confirms no call site in the
  remote extractor sets it. This code path needs live S3 and is only exercised by the `remote`-marked tests, so
  also confirm with `python -m pytest tests/ -m "not remote" -q` that nothing else regresses.

#### DC-3 — Ten of the eleven `ruff` ignore entries are inert

- **Location**: `pyproject.toml` (`[tool.ruff.lint] ignore`, the entries `PTH123`, `D203`, `D212`, `T201`, `FIX002`, `TD003`, `TD002`, `S101`, `ICN001`, `INP001`)
- **Issue**: `select = ["F", "E", "I"]` resolves to Pyflakes, pycodestyle errors, and isort. None of the ten
  listed rules belongs to those three linters, so none can ever be raised and none can ever be suppressed. The
  list reads as if the project enforces pathlib use, docstring conventions, and bandit checks, when it does not.
  `F821` is the one genuinely load-bearing entry and its comment is accurate.
- **Proposed change**: Reduce the `ignore` list to just the `F821` entry, keeping its existing comment.
- **Confidence it's safe**: **High**, verified by experiment rather than by reasoning about prefix matching. With
  the ten entries removed and only `F821` ignored, `ruff check .` reports "All checks passed!". With `F821` also
  removed, it reports two `F821 Undefined name geoip2` errors at `ip_utils/_geolite2.py:141` and
  `ip_utils/_resolver.py:131`, the lazy-import string annotations the comment refers to.
- **Verification**: `ruff check .` must still report "All checks passed!", and `pre-commit run --all-files` must
  pass.

#### DC-4 — Three unnecessary `pass` statements

- **Location**: `src/s3_log_extraction/_command_line_interface/_cli.py` (lines 180 `_config_cli`, 454 `_testing_cli`, 461 `_testing_generate_cli`)
- **Issue**: Each of these three group functions has a docstring, which already satisfies the body requirement,
  so the trailing `pass` is dead. The other group functions in the same file (`s3logextraction_cli`,
  `_cache_cli`, `_reset_cli`, `_update_cli`, `_update_ip_cli`) have no docstring and genuinely need their
  `pass`, which is what makes the inconsistency easy to miss.
- **Proposed change**: Delete the `pass` on lines 180, 454, and 461 only. Leave the other five.
- **Confidence it's safe**: **High**. `ruff check --select PIE790` flags exactly these three and no others. A
  `pass` after a docstring compiles to nothing.
- **Verification**: `ruff check --select PIE790 src/` returns clean, then `python -m pytest tests/test_cli_integration.py -q`.

#### DC-5 — An index assignment that cannot affect the output

- **Location**: `src/s3_log_extraction/summarize/_generate_summaries.py` (line 524, in `_summarize_dataset_by_day`)
- **Issue**: `summary_table.index = range(len(summary_table))` renumbers the index immediately before
  `to_csv(..., index=False)` on line 525, which does not write the index. The function returns `None` and the
  frame is local, so the assignment has no observable effect. The preceding `sort_values` on line 523 *is*
  observable and must stay. Note that `_summarize_dataset_by_asset` and `_summarize_dataset_by_region` write
  their frames without any such line, so this is a leftover rather than a convention.
- **Proposed change**: Delete line 524.
- **Confidence it's safe**: **High**. The only consumer of `summary_table` after line 524 is the `to_csv` call
  with `index=False`.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`. The golden files under
  `tests/expected_output/` pin the exact bytes of `by_day.tsv`, so any change in output fails the suite.

#### DC-6 — The `hatch-vcs` version source is never consulted

- **Location**: `pyproject.toml` (lines 2 `requires = [..., "hatch-vcs"]` and 5-6 `[tool.hatch.version] source = "vcs"`, against line 15 `version="1.11.3"`)
- **Issue**: Hatchling reads `[tool.hatch.version]` only when `version` appears in `[project].dynamic`. Here
  `[project]` sets a static `version` and never declares `dynamic`, so the VCS source is dead configuration. The
  installed distribution reports `1.11.3`, the literal. AGENTS.md instructing a manual bump of this field
  confirms the static value is the intended mechanism.
- **Proposed change**: Remove lines 5-6 and drop `hatch-vcs` from `build-system.requires`.
- **Confidence it's safe**: **Medium**. The evidence that the literal wins is direct (`pip show` reports
  `1.11.3` with no `dynamic` declared), but this touches the build backend, so it should be confirmed against a
  real build rather than by inspection, and it belongs in its own commit.
- **Verification**: `python -m build` before and after, then compare the `Version:` field of the generated
  `*.dist-info/METADATA` in both wheels. They must match.

---

### Duplication

#### DUP-1 — The activity-column coercion loop appears four times verbatim

- **Location**: `summarize/_generate_archive_summaries.py` (lines 57-61 and 92-96), `summarize/_generate_archive_totals.py` (lines 41-44), `summarize/_generate_all_dataset_totals.py` (lines 41-44)
- **Issue**: All four sites contain the identical four lines, comment included:

  ```python
  for column_name in ("number_of_requests", "number_of_downloads", "number_of_views"):
      if column_name not in summary.columns:  # Summarized before views were reported
          summary[column_name] = 0
      summary[column_name] = pandas.to_numeric(summary[column_name], errors="coerce").fillna(0).astype("int64")
  ```

  This is the backward-compatibility shim for summaries written before `number_of_views` existed. Four copies
  means a fifth summary type, or a change to the compatibility rule, has four places to touch and no single
  place that documents why the shim exists.
- **Proposed change**: Add one private helper to `summarize/_generate_summaries.py`, which both other modules
  already import from, and have all four sites call it:

  ```python
  def _coerce_activity_columns(summary_table: pandas.DataFrame, /) -> None:
      """Backfill the activity columns a summary written before views were reported lacks, and make them int64."""
  ```

  It mutates in place, exactly as the current loops do. The two sites inside
  `_generate_archive_summaries.py` keep their enclosing `for summary in ...:` loops and call the helper in the
  body.
- **Confidence it's safe**: **High**. The four bodies are textually identical; I compared them line by line. The
  helper performs the same operations in the same order on the same object, so the resulting frames are
  identical, including dtypes and column insertion order (new columns append in the same sequence).
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`, which covers both the archive and
  per-dataset totals paths and pins output bytes against `tests/expected_output/`.

#### DUP-2 — `_run_extraction` is byte-identical in both extractors

- **Location**: `extractors/_s3_log_access_extractor.py` (lines 220-232) and `extractors/_remote_s3_log_access_extractor.py` (lines 386-398)
- **Issue**: Thirteen lines, identical in every character including the `gawk --file` command string, the
  `_awk_env` mutation, and the `f"Extraction failed on {file_path}."` message. Both classes also hold identical
  `_relative_script_path` and `_awk_env` initialization (`_s3_log_access_extractor.py:53-54` and
  `_remote_s3_log_access_extractor.py:57-58`).
- **Proposed change**: Move the body into one private module-level function in `extractors/_utils.py`:

  ```python
  def _run_awk_extraction(*, script_path: pathlib.Path, file_path: pathlib.Path, awk_env: dict[str, str],
                          extraction_directory: pathlib.Path | None = None) -> None:
  ```

  Each class's `_run_extraction` then becomes a single delegating call, preserving both methods and their
  signatures so that nothing about either class's surface changes.
- **Confidence it's safe**: **High**. The two bodies are identical, so a single implementation cannot diverge
  from either. Keeping both `_run_extraction` methods in place preserves the method resolution both classes
  expose, including the `self._awk_env` mutation, which must stay on the instance dict.
- **Verification**: `python -m pytest tests/test_generic_extraction.py tests/test_cli_integration.py -q`
  exercises the local path end to end through gawk.

#### DUP-3 — Five identical `__hash__` bodies in the pre-validators

- **Location**: `validate/_downloads_logic_pre_validator.py` (lines 24-38), `validate/_extraction_heuristic_pre_validator.py` (21-27), `validate/_http_empty_split_pre_validator.py` (24-30), `validate/_http_split_count_pre_validator.py` (23-29), `validate/_timestamps_parsing_pre_validator.py` (20-26)
- **Issue**: All five compute the same thing in the same four statements: open
  `self._relative_awk_script_path` in binary mode, read it, take a SHA-1 hex digest, convert to `int` base 16.
  Only one of the five (`DownloadsLogicPreValidator`) documents what it does; the other four are bare.
- **Proposed change**: Add one private helper to `validate/_base_validator.py` and have each of the five
  `__hash__` methods delegate:

  ```python
  def _hash_awk_script_file(script_path: pathlib.Path, /) -> int:
      """Hash a validator's awk script, so that editing the rule starts a fresh validation record."""
  ```

  Keep the five `__hash__` methods themselves and move the single surviving docstring onto the helper.
- **Confidence it's safe**: **High**, and this one needs stating carefully because the hash value is load-bearing.
  `BaseValidator.__init__` names the record file
  `f"{self.__class__.__name__}_{hex(hash(self))[2:]}.txt"` (line 24), so a changed hash would orphan every
  existing record file and silently re-validate every log. The helper computes the same SHA-1 over the same
  bytes of the same file, so the integer is unchanged. Do **not** move this into
  `BaseValidator.__hash__`: that method has a different definition (it hashes
  `self._run_validation.__code__.co_code`) and `BaseValidator` is public API, so a downstream subclass may rely
  on it. See "Not Changed" below.
- **Verification**: `python -m pytest tests/test_downloads_logic_pre_validator.py tests/test_extraction_heuristic_pre_validator.py -q`,
  plus assert the record filename is stable:
  `python -c "from s3_log_extraction.validate import DownloadsLogicPreValidator as V; print(hex(hash(V()))[2:])"`
  before and after must print the same string.

#### DUP-4 — Five near-identical `_run_validation` bodies

- **Location**: the same five files as DUP-3 (`_run_validation` at lines 47-78, 40-59, 39-57, 38-56, 35-53 respectively)
- **Issue**: Each builds `awk --file <script> <log file>`, runs it through `subprocess.run(shell=True,
  capture_output=True, text=True)`, and raises `RuntimeError` with a five-line message on a non-zero return
  code. Only two things vary: the leading phrase of the message ("Downloads logic", "Extraction heuristic",
  "HTTP empty split", "HTTP split count", "Timestamps parsing") and the `env` argument.
- **Proposed change**: Add a second private helper beside the one from DUP-3:

  ```python
  def _run_awk_validation(*, script_path: pathlib.Path, file_path: pathlib.Path, failure_label: str,
                          environment_variables: dict[str, str] | None = None) -> None:
  ```

  Each subclass's `_run_validation` becomes a one-line call passing its own `failure_label`.
- **Confidence it's safe**: **Medium**, with one detail that must not be got wrong. Four of the five validators
  pass no `env`, which means `subprocess.run` inherits the parent environment.
  `ExtractionHeuristicPreValidator` passes `env={"EXCLUDED_IP_REGEX": self._excluded_ip_regex}` at
  `_extraction_heuristic_pre_validator.py:50`, which **replaces** the environment entirely rather than adding
  to it. The awk script reads that value from `ENVIRON`
  (`_extraction_heuristic_pre_validator_script.awk:4-8`), and the command still finds `awk` only because
  `/bin/sh` supplies a default `PATH`. The helper must therefore default `environment_variables` to `None` and
  forward it unchanged, never merge it with `os.environ`. The message strings must also stay byte-identical,
  including the odd trailing space in `f"\n{label} pre-check failed.\n "`.
- **Verification**: `python -m pytest tests/test_downloads_logic_pre_validator.py tests/test_extraction_heuristic_pre_validator.py -q`.
  The downloads-logic tests assert on the raised message, so a changed string fails. Additionally confirm the
  heuristic validator still works with a restricted environment, since that is the case the `env` detail
  protects.

#### DUP-5 — The S3 inventory walk is implemented twice

- **Location**: `utils/inventory.py`, `_read_s3_urls_from_local_inventory` (lines 320-341) and `get_log_bucket_stats` (lines 425-446)
- **Issue**: About fifteen lines repeat in both functions: the `pathlib.Path` coercion, the
  `_load_inventory_manifest` call, the identical `"Key" not in file_schema` check with the identical
  `f"'Key' column not found in inventory schema: {file_schema}"` message, the `key_index` lookup, the identical
  `symlink_lines` comprehension, and the identical nested loop that resolves each `data/*.csv.gz`, opens it with
  `gzip.open(..., "rt", newline="")`, and skips short rows with `if len(row) <= key_index: continue`.
- **Proposed change**: Extract a private generator and have both functions drive it:

  ```python
  def _iter_inventory_rows(inventory_directory: pathlib.Path, /) -> typing.Iterator[tuple[list[str], list[str], str]]:
      """Yield each row of the latest inventory snapshot, with the schema and source bucket it belongs to."""
  ```

  A generator rather than a list keeps the current memory behavior, which matters because these files hold
  millions of rows.
- **Confidence it's safe**: **High**. Both sites open the same files in the same order (`symlink_lines` order)
  and apply the same skip rule, so rows arrive in the same sequence. The `ValueError` message is identical in
  both, so a single copy in the helper preserves it. The one asymmetry is that `get_log_bucket_stats` also needs
  `size_index`, which it can still compute from the yielded schema.
- **Verification**: `python -m pytest tests/test_log_bucket_stats.py tests/test_remote_extractor_inventory.py -q`
  (about 90 tests covering both functions against synthetic inventory trees).

#### DUP-6 — The totals dictionary and requester parsing are duplicated

- **Location**: `summarize/_generate_archive_totals.py` (lines 59-71) and `summarize/_generate_all_dataset_totals.py` (lines 51-65)
- **Issue**: The seven-key totals dict is character-for-character identical in both, as is the requester-count
  parsing `if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<")`. These seven
  keys are the published schema of `totals.json` and `archive_totals.json`, so having them written out twice is
  precisely where a schema drift would hide.
- **Proposed change**: Extract one private helper into `summarize/_generate_summaries.py`, which both modules
  already import from:

  ```python
  def _build_totals(*, summary_table: pandas.DataFrame, number_of_unique_regions: int,
                    number_of_unique_countries: int, number_of_requesters: str | int) -> dict[str, str | int]:
  ```
- **Confidence it's safe**: **High**. Both dicts are identical literals over identically named locals, and both
  are serialized with `json.dump(..., indent=2, sort_keys=True)`, so even key ordering is normalized by the
  writer.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`, which asserts on the contents of both
  JSON files.

#### DUP-7 — `[int(value.strip()) for value in X.read_text().splitlines()]` six times

- **Location**: `summarize/_generate_summaries.py` lines 494, 498, 549, 556, 608, 612
- **Issue**: Six identical comprehensions reading a per-request integer file. Paired with the `timestamps`
  variant at line 489 and the non-empty-line variants at lines 208-209, the module has four spellings of "read
  this extraction file".
- **Proposed change**: Add `def _read_integers_from_file(file_path: pathlib.Path, /) -> list[int]:` and use it
  at all six sites.
- **Confidence it's safe**: **High**. All six are the same expression over the same shape of input. `int()` on a
  stripped string raises `ValueError` identically from inside a helper.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`.

#### DUP-8 — The optional-path conversion appears nine times in the CLI

- **Location**: `_command_line_interface/_cli.py` lines 122, 171, 226, 268, 302, 400, 442, 554, 618
- **Issue**: `pathlib.Path(cache_directory) if cache_directory is not None else None` is written out nine times,
  sometimes assigned to `cache_path` and sometimes inlined into the call, which makes the commands read
  differently for no reason.
- **Proposed change**: Add `def _optional_path(value: str | None, /) -> pathlib.Path | None:` at module level
  and call it at all nine sites.
- **Confidence it's safe**: **High**. A pure expression with no side effects, identical at every site.
- **Verification**: `python -m pytest tests/test_cli_integration.py tests/test_log_bucket_stats.py -q`.

#### DUP-9 — The `--cache` option declaration is repeated six times identically

- **Location**: `_command_line_interface/_cli.py` lines 152-162, 214-224, 243-253, 276-286, 365-375, 426-436
- **Issue**: Six `rich_click.option` blocks share the same flag, the same destination name, the same
  `Path(writable=True, file_okay=False, dir_okay=True)` type, the same default, and the same two-sentence help
  text "Use a non-default cache directory for this command. This overrides the configured cache directory
  without modifying saved config." That is about 60 lines expressing one idea. Three further `--cache` blocks
  (lines 61-71, 525-535, 598-608) have genuinely different help text or a different `exists=True` type and must
  stay separate.
- **Proposed change**: Define one module-level decorator that applies the shared option, and use it on the six
  commands that take the identical variant:

  ```python
  def _cache_directory_option(command):
      """Attach the shared `--cache` option to a command."""
  ```
- **Confidence it's safe**: **Medium**. The construction is the standard click idiom and the option object is
  built from the same arguments, so the parsed result is unchanged. The risk is entirely in `--help` rendering:
  option order within a command must be preserved, because a decorator applied in a different position reorders
  the help listing. Apply the decorator in the same position the literal block occupied.
- **Verification**: Capture `--help` for every command before and after and diff them:
  `for c in "stop" "reset extraction" "update ip database" "update ip coordinates" "update summaries" "update totals"; do s3logextraction $c --help; done > /tmp/help.txt`
  must produce a byte-identical file.

#### DUP-10 — `_summarize_dataset_by_day` and `_summarize_dataset_by_region` are twins

- **Location**: `summarize/_generate_summaries.py` (lines 465-525 and 578-640)
- **Issue**: About 45 lines of the two functions are the same algorithm with one column renamed. Both accumulate
  a key list alongside `all_bytes_sent` and `all_downloads`, both read `bytes_sent.txt` and `download.txt` with
  the identical four lines, both run the identical three-`defaultdict` accumulation loop, both bail on an empty
  result, and both build a five-column frame whose last four columns are identical.
- **Proposed change**: Extract the shared tail into a private helper that returns the assembled frame, leaving
  each caller to supply its key column and to do its own writing:

  ```python
  def _assemble_activity_summary(*, keys: list[str], bytes_sent: list[int], downloads: list[int],
                                 number_of_views_by_key: dict[str, int], key_column_name: str) -> pandas.DataFrame | None:
  ```
- **Confidence it's safe**: **Medium**. The aggregation is identical, but three differences must survive the
  extraction. `_summarize_dataset_by_day` calls `sort_values(by="date")` and `_summarize_dataset_by_region`
  does not sort at all, so row order differs and the sort must stay at the call site.
  `_summarize_dataset_by_day` calls `summary_file_path.parent.mkdir` before writing and
  `_summarize_dataset_by_region` leaves that to `_write_summary_by_region`. And the writers differ, one being
  `to_csv` and the other the disclosure-gated `_write_summary_by_region`. Do this one after DUP-7, which removes
  part of the duplication on its own.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`. The golden `by_day.tsv` and
  `by_region.tsv` files pin row order exactly, so a sorting mistake fails loudly.

#### DUP-11 — The archive by-day and by-region aggregations are twins

- **Location**: `summarize/_generate_archive_summaries.py` (lines 52-84 and 87-123)
- **Issue**: Both blocks glob the per-dataset summaries, filter out the `archive` directory with the identical
  `if ....parent.name != "archive"` guard, concatenate, group by one column, `natsort` the group key, reindex to
  a fixed five-column order, and cast four columns to `int64`. Only the glob pattern, the group key, and the
  writer differ. This is the duplication the file's own `# TODO` on line 50 refers to.
- **Proposed change**: Extract
  `def _aggregate_dataset_summaries(*, summary_directory: pathlib.Path, pattern: str, key_column_name: str) -> pandas.DataFrame | None`
  and call it twice.
- **Confidence it's safe**: **Medium**. One asymmetry must be preserved exactly: the by-region block is wrapped
  in `if all_dataset_summaries_by_region:` (line 99) and the by-day block is not, so an archive with no
  published by-region summary currently skips the region work while an archive with no by-day summary raises
  from `pandas.concat`. That difference is existing behavior and must not be "fixed" while consolidating.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`, which covers the archive mode
  including the withheld-by-region case.

#### DUP-12 — The child-process copy-back loop is duplicated

- **Location**: `extractors/_s3_log_access_extractor.py` (lines 151-169) and `extractors/_remote_s3_log_access_extractor.py` (lines 166-184)
- **Issue**: The `tqdm` block labelled "Copying files from child processes" is identical in both, down to all six
  progress-bar keyword arguments, and its body (lines 160-169 and 175-184) is byte-for-byte identical: strip the
  PID component off the relative path, rebuild the destination, `mkdir(parents=True, exist_ok=True)`, call
  `_merge_file_into_extraction`, `unlink`.
- **Proposed change**: Extract the loop body into `extractors/_utils.py`:

  ```python
  def _merge_worker_output_into_extraction(*, temporary_directory: pathlib.Path,
                                           extraction_directory: pathlib.Path, use_encryption: bool) -> None:
  ```
- **Confidence it's safe**: **High** for the loop body, which is identical. Note that the two `files_to_copy`
  expressions that *feed* the loop are **not** identical: the remote extractor filters with
  `if path.is_file() is True` (line 164) and the local one does not (line 150). Leave that expression at each
  call site, or pass the file list in. Do not unify it.
- **Verification**: `python -m pytest tests/test_generic_extraction.py::test_extraction_parallel tests/test_cli_integration.py::test_cli_extraction_parallel -q`.

#### DUP-13 — `_write_by_region_summary` is duplicated between two test modules

- **Location**: `tests/test_ip_utils.py` (lines 69-75) and `tests/test_remote.py` (lines 44-50)
- **Issue**: Identical seven-line helper, docstring included, in two files. It also hardcodes the by-region
  column header, so the published schema is restated in two test files.
- **Proposed change**: Move it to `tests/conftest.py` as a fixture or a module-level helper and import it in
  both places.
- **Confidence it's safe**: **High**. Byte-identical definitions; moving one to a shared location changes no
  assertion.
- **Verification**: `python -m pytest tests/test_ip_utils.py -q` and `python -m pytest tests/test_remote.py --collect-only -q`
  (the latter is `remote`-marked and cannot run here, so at least confirm it still collects).

#### DUP-14 — `_build_inventory_directory` is implemented twice with drifted signatures

- **Location**: `tests/test_log_bucket_stats.py` (line 19) and `tests/test_remote_extractor_inventory.py` (line 62)
- **Issue**: Both build a synthetic S3 inventory tree, and the bodies largely agree, but the signatures have
  drifted. One takes `rows: list[tuple]` with a required `file_schema`; the other takes `keys: list[str]` with
  `file_schema: str = "Bucket, Key"` defaulted. The second is the special case of the first where every row is
  `(source_bucket, key)`.
- **Proposed change**: Keep the general `rows` form in `tests/conftest.py` and express the `keys` form in terms
  of it, then update the nine call sites across the two files.
- **Confidence it's safe**: **Medium**. Behavior-neutral in principle, since one is a special case of the other,
  but it touches nine call sites in two modules and the two bodies must be diffed in full first to confirm the
  generated trees are identical.
- **Verification**: `python -m pytest tests/test_log_bucket_stats.py tests/test_remote_extractor_inventory.py -q`
  (about 90 tests).

#### DUP-15 — The validator protocol list is written out three times

- **Location**: `_command_line_interface/_cli.py` (line 481 the `Choice` list, lines 486-488 the `Literal` annotation, lines 492-507 the `match` arms)
- **Issue**: The same five protocol names appear three times in one function, and the `match` has five arms that
  differ only in the class they instantiate.
- **Proposed change**: Define one module-level mapping, `_PRE_VALIDATORS: dict[str, type] = {...}`, and drive all
  three from it: `Choice(choices=list(_PRE_VALIDATORS))` for the option, and
  `_PRE_VALIDATORS[protocol]().validate_directory(directory=directory)` for the body.
- **Confidence it's safe**: **Medium-High**. Dict insertion order preserves the `Choice` order, so `--help`
  output is unchanged. One behavior detail: the current `match` has no `case _`, so an unmatched protocol would
  silently do nothing, whereas a dict lookup would raise `KeyError`. That path is unreachable because
  `rich_click.Choice` rejects unknown values before the body runs, but it is the one difference to be aware of.
  The `Literal` annotation can keep its explicit form, since it is documentation rather than runtime behavior.
- **Verification**: `s3logextraction validate --help` must be byte-identical, and
  `python -m pytest tests/test_cli_integration.py -q`.

---

### Consolidation

#### CON-1 — Move the two function-local `config` imports to module top

- **Location**: `utils/inventory.py` (line 126 inside `get_ip_stats`, line 484 inside `get_extraction_completion`)
- **Issue**: AGENTS.md requires imports at the top of the file, with breaking a circular dependency as the only
  exception. These two are the only function-local imports in `src/` that are not lazy guards for an optional
  extra. The other five (`requests`, `geoip2`, `fsspec`) are legitimate, since the package must import without
  the `geolocation` and `remote` extras installed.
- **Proposed change**: Replace both with a single `from ..config import get_cache_directory, get_cache_subdirectory`
  at the top of the module.
- **Confidence it's safe**: **High**, verified by experiment rather than by reasoning about the import graph. I
  applied the change to a scratch copy and `import s3_log_extraction; from s3_log_extraction.utils import get_ip_stats, get_extraction_completion`
  succeeded. There is no cycle to break: `config/` imports nothing from `utils/`, and `config` is in any case
  already reachable at import time through `utils/inventory.py`'s existing top-level import of
  `..ip_utils._resolver`, which transitively imports `..config`.
- **Verification**: `python -c "import s3_log_extraction"` then `python -m pytest tests/ -m "not remote" -q`.

#### CON-2 — Import from the public surface in three test modules

- **Location**: `tests/test_ip_utils.py` (line 22), `tests/test_log_bucket_stats.py` (lines 14, 16)
- **Issue**: AGENTS.md asks tests to import what is publicly exposed through `__init__.py`. Three imports reach
  into private modules for names that are already re-exported: `country_alpha_2_to_alpha_3` and
  `get_region_coordinates` are both in `ip_utils.__all__`; `get_extraction_completion`, `get_ip_stats`, and
  `get_log_bucket_stats` are all in `utils.__all__`; `s3logextraction_cli` is in the top-level `__all__`.
- **Proposed change**: Rewrite those three import statements against the public packages
  (`from s3_log_extraction.ip_utils import ...`, `from s3_log_extraction.utils import ...`,
  `from s3_log_extraction import s3logextraction_cli`).
- **Confidence it's safe**: **High**. Each public name is bound to the identical object, so the tests exercise the
  same code. Two imports must **not** be changed: `tests/test_log_bucket_stats.py:13`
  (`import ..._cli as cli_module`) exists so that monkeypatching targets the importing module's binding, which
  AGENTS.md explicitly prescribes, and `tests/test_remote_extractor_inventory.py:13` imports
  `_extract_date_from_log_filename`, which is genuinely private and has no public alias.
- **Verification**: `python -m pytest tests/test_ip_utils.py tests/test_log_bucket_stats.py -q`.

#### CON-3 — Replace the `deque(..., maxlen=0)` drain in `reset_extraction`

- **Location**: `config/_reset.py` (lines 22-28)
- **Issue**: Three separate oddities in seven lines. A list comprehension that only copies its iterable
  (`[record for record in itertools.chain(...)]`, which `ruff --select C416` flags), then
  `collections.deque((record.unlink(missing_ok=True) for record in records), maxlen=0)` used purely to consume a
  generator for its side effects. A plain `for` loop says the same thing in two lines and removes the need for
  both the `collections` and `itertools` imports.
- **Proposed change**:

  ```python
  for record in itertools.chain(
      records_directory.glob("*_extraction.log"), records_directory.glob("*_file-processing-*.txt")
  ):
      record.unlink(missing_ok=True)
  ```

  Then drop `import collections` (line 1). Keep `import itertools`.
- **Confidence it's safe**: **High**. Both forms unlink exactly the same paths in the same order. The current
  code materializes the glob results into a list before unlinking; the loop above consumes the chain lazily.
  That difference is not observable here, because `unlink` on a file does not affect which paths a
  directory glob has already yielded, but if you prefer to be conservative, keep `list(...)` around the chain.
- **Verification**: `python -m pytest tests/ -m "not remote" -q -k "reset or cli"`.

#### CON-4 — Pass the extraction path in rather than deriving it from three parents

- **Location**: `summarize/_generate_summaries.py` (lines 534-535, in `_summarize_dataset_by_asset`)
- **Issue**: The function recovers `dataset_id` from `summary_file_path.parent.name` and then the extraction root
  from `summary_file_path.parent.parent.parent / "extraction" / dataset_id`, with a comment admitting
  `# Assumes same cache dir`. The sole caller, `_summarize_dataset`, holds both `dataset_id` and
  `summary_directory` already, so the function is reverse-engineering information it could simply be handed.
- **Proposed change**: Add `dataset_id` and `extraction_directory` parameters and pass them from
  `_summarize_dataset`, threading `extraction_directory` down from `generate_summaries`, which computes it at
  line 376. Both functions are private, so the signature change is internal.
- **Confidence it's safe**: **Medium**. The derived value must be reproduced exactly:
  `cache/summaries/<dataset>/by_asset.tsv` walks up three parents to `cache`, so the equivalent explicit value
  is `cache_dir / "extraction" / dataset_id`. Worth doing because the `asset_path` column of `by_asset.tsv` is
  computed by `relative_to` against this path, so an error here silently corrupts a published column rather than
  raising.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`. The golden `by_asset.tsv` files pin
  the `asset_path` values.

#### CON-5 — Lift the two closures out of `get_ip_stats`

- **Location**: `utils/inventory.py` (lines 138-151 `_categorize`, lines 164-165 `_pct`)
- **Issue**: `_categorize` is a 14-line `match` statement nested inside a public function. It closes over nothing,
  so it is redefined on every call and cannot be tested directly. `_pct` does close over `extracted_ip_count`,
  and its parameter is named `n`.
- **Proposed change**: Move `_categorize` to module level as a private function. Either leave `_pct` as a closure
  or make it a module-level `_percent_of(count: int, /, *, total: int) -> float`. Rename `n` to `count`.
- **Confidence it's safe**: **High** for `_categorize`, which captures no enclosing variable, so hoisting it
  cannot change its result. **Medium** for `_pct`, only because it means threading `extracted_ip_count` through
  at six call sites.
- **Verification**: `python -m pytest tests/test_log_bucket_stats.py -q`.

#### CON-6 — Cache the subregion-to-CIDR index instead of rebuilding it per region

- **Location**: `ip_utils/_update_region_code_coordinates.py` (line 119, in `_get_service_coordinates_from_geolite2`)
- **Issue**: `subregion_to_cidr_address = {subregion: cidr_address for cidr_address, subregion in cidr_addresses_and_subregions}`
  rebuilds a dictionary over the service's entire published range list on every call, once per new cloud region.
  The AWS list is several thousand entries. The underlying list is already `lru_cache`d by
  `_get_cidr_address_ranges_and_subregions`, so only the dict construction repeats. Separately, the
  comprehension variable `subregion` shadows the function-level `subregion` bound on line 114, which reads
  confusingly even though comprehension scoping keeps them separate.
- **Proposed change**: Move the inversion into its own `functools.cache`-decorated private helper keyed on
  `service_name`, and rename the comprehension variables so they do not shadow.
- **Confidence it's safe**: **Medium**. The mapping is deterministic for a given service and last-write-wins on
  duplicate subregions, so a cached copy yields the same lookups. The caveat is that the cached dict would then
  be shared, so it must not be mutated, and nothing currently mutates it.
- **Verification**: `python -m pytest tests/test_ip_utils.py -q`.

#### CON-7 — Give the pre-validator `__init__` methods their return annotation

- **Location**: all five files in `validate/`, `def __init__(self):` at lines 41, 30, 33, 32, 29 respectively
- **Issue**: `BaseValidator.__init__` and `RemoteS3BucketValidator.__init__` are both annotated `-> None`; the
  five subclasses are not. The same five also each carry a duplicated `# TODO: parallelize` and
  `# TODO: does this hold after bundling?`.
- **Proposed change**: Add `-> None` to the five. Leave the TODO comments, which are deliberate markers.
- **Confidence it's safe**: **High**. Annotations are not enforced here; none of these classes is `beartype`
  decorated.
- **Verification**: `ruff check .` and `python -m pytest tests/ -m "not remote" -q`.

---

### Cleanup

#### CL-1 — 21 assignments immediately before a `return`

- **Location**: `validate/_base_validator.py:19`; `validate/_downloads_logic_pre_validator.py:38`;
  `validate/_extraction_heuristic_pre_validator.py:27`; `validate/_http_empty_split_pre_validator.py:30`;
  `validate/_http_split_count_pre_validator.py:29`; `validate/_timestamps_parsing_pre_validator.py:26`;
  `utils/encryption.py:109, 130, 151`; `ip_utils/_ip_utils.py:66, 70, 74, 87, 118, 125, 133, 139`;
  `ip_utils/_ip_cache.py:37`; `extractors/_stop.py:22`; `extractors/_remote_s3_log_access_extractor.py:275, 338`;
  `config/_config.py:42`
- **Issue**: Each assigns a value to a local and returns it on the next line. The eight in `_ip_utils.py` are
  the worst case: each `match` arm invents a uniquely named local (`github_cidr_request`, `aws_cidr_request`, and
  so on) purely to return it, which triples the height of both functions for no information gain.
- **Proposed change**: Return the expression directly at all 21 sites.
- **Confidence it's safe**: **High**, flagged mechanically by `ruff check --select RET504`, which finds exactly
  these 21 and nothing else. In each case the local is read exactly once, on the following line.
- **Verification**: `ruff check --select RET504 src/` returns clean, then `python -m pytest tests/ -m "not remote" -q`.

#### CL-2 — Four unnecessary set comprehensions

- **Location**: `extractors/_s3_log_access_extractor.py` (lines 59, 62) and `extractors/_remote_s3_log_access_extractor.py` (lines 218, 221)
- **Issue**: `{file_path for file_path in X.read_text().splitlines()}` is `set(X.read_text().splitlines())`.
  Note the bound name is `file_path` in all four even though the remote pair holds S3 URL basenames, not paths.
- **Proposed change**: Rewrite the four as `set(...)` calls.
- **Confidence it's safe**: **High**, flagged by `ruff check --select C416`. Identical resulting sets.
- **Verification**: `ruff check --select C416 src/` returns clean, then
  `python -m pytest tests/test_generic_extraction.py tests/test_remote_extractor_inventory.py -q`.

#### CL-3 — Use `TIMESTAMP_FORMAT` instead of repeating its literal

- **Location**: `summarize/_generate_summaries.py` (line 488)
- **Issue**: The module imports `TIMESTAMP_FORMAT` from `.globals` at line 12 and uses it at line 230, but line
  488 hardcodes `"%y%m%d%H%M%S"`, which is the identical string. The extraction timestamp format is therefore
  defined in two places, one of which is invisible to anyone changing the constant.
- **Proposed change**: Replace the literal on line 488 with `TIMESTAMP_FORMAT`.
- **Confidence it's safe**: **High**. `summarize/globals.py:7` defines `TIMESTAMP_FORMAT = "%y%m%d%H%M%S"`,
  character-for-character the literal on line 488, and the name is already imported in this module.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`.

#### CL-4 — A redundant `str()` around a value that is already `str`

- **Location**: `summarize/_generate_summaries.py` (line 488)
- **Issue**: `datetime.datetime.strptime(str(timestamp.strip()), ...)` where `timestamp` comes from
  `read_text().splitlines()` and is therefore already a `str`, as is `.strip()` of it. Compare line 230, which
  calls `strptime(timestamp, TIMESTAMP_FORMAT)` with no conversion.
- **Proposed change**: Use `timestamp.strip()` directly.
- **Confidence it's safe**: **High**. `str()` of a `str` returns the same object.
- **Verification**: Same as CL-3; these two findings are one edit.

#### CL-5 — `any()` used as a non-emptiness test in three places

- **Location**: `config/_config.py:18` (`if not any(config):`), `extractors/_stop.py:50` (`if any(get_running_pids()):`), `ip_utils/_update_region_code_coordinates.py:94` (`if any(unresolved_region_codes):`)
- **Issue**: In all three the intent is "is this container non-empty", but `any()` tests the truthiness of the
  *elements*. It also reads as though element truthiness mattered. `_stop.py` is the clearest case of drift: line
  32 of the same function already writes `if len(running_pids) == 0:` for the same question. Note that
  `testing/_benchmarking.py:87` uses `any(benchmark_directory.iterdir())` correctly, since that consumes a
  generator.
- **Proposed change**: Use the container directly: `if not config:`, `if get_running_pids():`,
  `if unresolved_region_codes:`.
- **Confidence it's safe**: **Medium** for the two list cases, where elements are PID strings and region codes
  that are never empty, so the two forms agree. **Low** for `config/_config.py:18`, which differs for a config
  dict whose only key is falsy, such as `{"": x}`. That cannot arise from `set_cache_directory`, the only
  writer, but it is a real semantic difference, so treat that one site as needing a maintainer's sign-off.
- **Verification**: `python -m pytest tests/ -m "not remote" -q`, and for the `_config.py` site confirm no caller
  can produce a falsy key: `grep -rn "save_config" src/ tests/`.

#### CL-6 — An f-string wrapper around an expression that is already a string

- **Location**: `extractors/_remote_s3_log_access_extractor.py` (line 300)
- **Issue**: `months = {f"{line.split(" ")[-1].rstrip("/\n")}" for line in months_result.splitlines()}`. The
  f-string contains one replacement field and nothing else, so it is an identity conversion on a `str`. Line 289
  writes the same expression without the wrapper, so the file is inconsistent with itself.
- **Proposed change**: Drop the f-string: `{line.split(" ")[-1].rstrip("/\n") for line in months_result.splitlines()}`.
- **Confidence it's safe**: **High**. `f"{s}"` for a `str` `s` yields an equal string. Lines 310 and 334 also use
  f-strings but genuinely interpolate more than one value, so leave those.
- **Verification**: Remote-only code path; confirm `python -m pytest tests/ -m "not remote" -q` still passes and
  that `tests/test_remote.py` still collects.

#### CL-7 — `msg` where the rest of the codebase uses `message`

- **Location**: `summarize/_generate_archive_totals.py` (line 52)
- **Issue**: Every `raise` in the package binds its text to `message`, including line 34 of this very function.
  Line 52 uses `msg`.
- **Proposed change**: Rename the local to `message`.
- **Confidence it's safe**: **High**. A local variable with one read, on the `raise` that follows it.
- **Verification**: `grep -rn "msg" src/` should return nothing, then `python -m pytest tests/test_generic_summaries.py -q`.

#### CL-8 — `io` used as a file-handle variable name

- **Location**: `summarize/_generate_archive_totals.py` (line 74) and `summarize/_generate_all_dataset_totals.py` (line 68)
- **Issue**: `with archive_totals_file_path.open(mode="w") as io:` shadows the name of the stdlib `io` module
  inside the block. Everywhere else in the package the same handle is called `file_stream`.
- **Proposed change**: Rename both to `file_stream`.
- **Confidence it's safe**: **High**. Neither module imports `io`, so nothing is actually shadowed today; the
  rename is purely for consistency and to remove the trap.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`.

#### CL-9 — `for key in dict.keys()`

- **Location**: `testing/_assertions.py` (line 69)
- **Issue**: `for relative_file_path in relative_expected_file_contents.keys():` iterates the keys view
  explicitly where iterating the dict does the same thing.
- **Proposed change**: Drop `.keys()`.
- **Confidence it's safe**: **High**, flagged by `ruff check --select SIM118`. Iteration order is identical.
- **Verification**: `ruff check --select SIM118 src/` returns clean, then `python -m pytest tests/ -m "not remote" -q`.

#### CL-10 — `get_running_pids` is annotated `list[str]` but returns `set[str]`

- **Location**: `extractors/_stop.py` (line 11)
- **Issue**: The body builds a set comprehension and subtracts a set, so the return value is a `set[str]`. The
  annotation says `list[str]`. `get_running_pids` is public (`extractors.__all__`), so the annotation is part of
  what a reader trusts. The incorrect annotation also disguises why `stop_extraction` has to write
  `list(running_pids)[0]` on line 37.
- **Proposed change**: Change the annotation to `set[str]`.
- **Confidence it's safe**: **High**. No `beartype` decorator on this function, so the annotation is not enforced
  at runtime, and correcting it cannot raise. The returned object is unchanged.
- **Verification**: `python -c "from s3_log_extraction.extractors import get_running_pids; print(type(get_running_pids()))"`
  prints `<class 'set'>`, then `python -m pytest tests/ -m "not remote" -q`.

#### CL-11 — `_request_cidr_range` is annotated `-> dict` but one branch returns a list

- **Location**: `ip_utils/_ip_utils.py` (line 58)
- **Issue**: The `"GitHub"`, `"AWS"`, and `"GCP"` arms return parsed JSON objects, but the `"VPN"` arm returns
  `.content.decode("utf-8").splitlines()`, which is a `list[str]`. The annotation claims `dict`.
- **Proposed change**: Annotate `-> dict | list[str]`.
- **Confidence it's safe**: **High**. Not enforced at runtime; nothing about the returned values changes.
- **Verification**: `python -m pytest tests/test_ip_utils.py -q`, which patches this function by name and
  exercises all four service arms.

#### CL-12 — `_validate_cli` annotates its `directory` parameter as `pathlib.Path`

- **Location**: `_command_line_interface/_cli.py` (line 489)
- **Issue**: The argument is declared as `rich_click.Path(writable=False)` with no `path_type`, so click passes a
  `str`. Every other command in the file correctly annotates `directory: str` (for example line 106 and line
  466). Only this one claims `pathlib.Path`.
- **Proposed change**: Change the annotation to `str`.
- **Confidence it's safe**: **High**. Annotations are not enforced in this module and the value click passes does
  not change. `BaseValidator.validate_directory` coerces with `pathlib.Path(directory)` on its own anyway.
- **Verification**: `python -m pytest tests/test_cli_integration.py -q`.

#### CL-13 — Two spellings of the same date validation in one module

- **Location**: `utils/inventory.py`, `_extract_date_from_log_filename` (lines 197-205) against the inline check in `_read_s3_urls_from_local_inventory` (lines 354-364)
- **Issue**: Both validate that three components are a 4-digit year, a 2-digit month, and a 2-digit day, then
  format them as `YYYY-MM-DD`. The helper does it in eight compact lines; the inline version spells the same
  conditions out across nine lines of a single `if`.
- **Proposed change**: Extract `def _format_date_if_valid(year: str, month: str, day: str) -> str | None:` and
  have both use it. `_extract_date_from_log_filename` keeps its name and signature, since
  `tests/test_remote_extractor_inventory.py` imports and tests it directly.
- **Confidence it's safe**: **Medium**. The two guards are not identical in their surroundings: the helper
  requires `len(parts) >= 3` and the inline version requires `len(parts) >= 4`. Those length checks must stay at
  their call sites; only the three-component validation moves.
- **Verification**: `python -m pytest tests/test_remote_extractor_inventory.py -q`, which has dedicated
  parametrized cases for the filename strategy.

#### CL-14 — A redundant `total=` on a sized iterable

- **Location**: `ip_utils/_update_region_code_coordinates.py` (line 60)
- **Issue**: `tqdm.tqdm(iterable=region_codes_to_update, total=len(region_codes_to_update), ...)` where
  `region_codes_to_update` is a `set`, from which tqdm already derives the length.
- **Proposed change**: Drop the `total` argument.
- **Confidence it's safe**: **Medium**. tqdm falls back to `len(iterable)` when `total` is omitted and the
  iterable is sized, so the rendered bar is unchanged. Rated medium only because it touches user-visible progress
  output, which is the sort of thing worth eyeballing once rather than trusting. Several other call sites in the
  package pass a redundant `total` the same way; they can follow in the same commit or be left alone.
- **Verification**: Run `s3logextraction update ip coordinates` against a populated cache and confirm the bar
  still shows a denominator.

#### CL-15 — `_collect_unique_ips` takes three positional parameters

- **Location**: `summarize/_generate_summaries.py` (lines 245-248)
- **Issue**: AGENTS.md requires keyword-only parameters via `(*, ...)` for multi-input functions. This private
  helper takes three and marks none. Both of its call sites (lines 314-316 and 409-412) already pass everything
  by keyword.
- **Proposed change**: Insert `*,` after the opening parenthesis.
- **Confidence it's safe**: **High**. Private function, two call sites, both already keyword-only in practice.
  Confirmed with `grep -rn "_collect_unique_ips" src/ tests/`.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`.

#### CL-16 — A redundant `isinstance` on a value that is always `str`

- **Location**: `summarize/_generate_archive_totals.py` (line 60)
- **Issue**: `number_of_requesters` is assigned on line 59 from `read_text().strip()`, so it is unconditionally a
  `str` by line 60, making `isinstance(number_of_requesters, str) and` always true. The `str | int` annotation
  on line 59 is what makes the check look necessary. The visually identical line in
  `_generate_all_dataset_totals.py:54` **is** load-bearing, because there the value can be the integer `0`.
- **Proposed change**: Drop the `isinstance` conjunct in `_generate_archive_totals.py` only, leaving
  `if not number_of_requesters.startswith("<"):`. Leave `_generate_all_dataset_totals.py:284` untouched.
- **Confidence it's safe**: **Medium**. The deduction is sound for the current code, but the symmetry between the
  two modules has some documentary value, and DUP-6 proposes extracting the shared helper that would resolve
  this more cleanly. Prefer doing DUP-6 and letting this fall out of it.
- **Verification**: `python -m pytest tests/test_generic_summaries.py -q`.

#### CL-17 — Five passes over the password to estimate its entropy

- **Location**: `utils/encryption.py` (lines 32-41, in `_estimate_entropy_bits`)
- **Issue**: Five separate `any(character in string.X for character in password)` generator scans, each walking
  the whole password. One pass building `set(password)` and intersecting it against each character class says the
  same thing once.
- **Proposed change**: Compute `distinct = set(password)` once and test each class against it. Note that
  `validate_password_strength` already computes `len(set(password))` on line 59, so the set is built twice in the
  same call path.
- **Confidence it's safe**: **Medium**. Set membership over the same classes yields the same five booleans, so
  `charset_size` and the returned float are identical. Rated medium rather than high because this function gates
  encryption and its float result is compared against `_MINIMUM_ENTROPY_BITS`, so it deserves a direct
  before-and-after comparison rather than only a test run.
- **Verification**: `python -m pytest tests/test_encryption.py -q`, plus a spot check that
  `_estimate_entropy_bits` returns the identical float for a handful of inputs spanning all five classes.

#### CL-18 — `dict.fromkeys` for a constant-valued dict comprehension

- **Location**: `summarize/_generate_archive_summaries.py` (line 151); also `tests/test_generic_summaries.py` (lines 614, 930, two occurrences each)
- **Issue**: `{column_name: "int64" for column_name in asset_type_columns}` builds a dict whose value does not
  depend on the key.
- **Proposed change**: `dict.fromkeys(asset_type_columns, "int64")`.
- **Confidence it's safe**: **High**, flagged by `ruff check --select C420`. Identical dict, identical insertion
  order. The string value is immutable, so sharing it across keys is safe.
- **Verification**: `ruff check --select C420 src/ tests/` returns clean, then `python -m pytest tests/test_generic_summaries.py -q`.

---

## Needs Verification

These are observations I can support but cannot resolve alone, because doing so would change behavior or
requires a decision only the maintainer can make. None is proposed as a change.

**NV-1 — `update summaries` accepts three options it silently ignores.** `--pick` (lines 320-326), `--skip`
(327-333), and `--workers` (334-344) are declared on `_update_summaries_cli` and bound to parameters (lines
385-387), but the function body (lines 400-414) never reads `pick`, `skip`, or `workers`. A user who passes
`--workers 8` gets no parallelism and no warning. **Question for a maintainer:** are these reserved for planned
work, or leftovers? Removing them changes the CLI surface, so it is out of scope here. To confirm:
`grep -n "pick\|skip\|workers" src/s3_log_extraction/_command_line_interface/_cli.py | sed -n '/_update_summaries_cli/,$p'`,
and check the issue tracker for a planned filtering feature.

**NV-2 — An operator-precedence defect in `_handle_aws_credentials`.** At `extractors/_utils.py:102`,
`if aws_access_key_id is None or aws_secret_access_key is None and aws_credentials_file_path.exists():` parses as
`A is None or (B is None and exists())` because `and` binds tighter than `or`. When the access key is unset but
the credentials file does not exist, the block is still entered and line 103 opens a file that may not be there.
Line 115 compounds it: `next(line.strip() for line in ...)` takes the *first line of the file* as the access key
with no `"aws_access_key_id" in line` filter, while line 116 does filter for the secret. **Question:** is the
intended condition `(A is None or B is None) and exists()`? Fixing it changes behavior and belongs in its own
bug-fix pull request, not this review. To confirm: construct an `~/.aws/credentials` absent case with
`AWS_ACCESS_KEY_ID` unset and observe the `FileNotFoundError`.

**NV-3 — `get_key()` runs 600,000 PBKDF2 iterations on every encrypt and decrypt.** `encrypt_bytes` (line 126)
and `decrypt_bytes` (line 147) each call `get_key()`, which derives the key afresh. Because
`_merge_file_into_extraction` encrypts and decrypts per asset file, a large extraction pays this cost thousands
of times. Memoizing `get_key` would be a large speedup but is **not** behavior-neutral: it would stop honoring a
mid-run change to `S3_LOG_EXTRACTION_PASSWORD` or `S3_LOG_EXTRACTION_SALT`, and it would cache a secret in
process memory for the process lifetime. **Question:** is a per-process key cache acceptable? To measure:
`python -m timeit -n 5 -s "import os; os.environ['S3_LOG_EXTRACTION_PASSWORD']='x'*32; from s3_log_extraction.utils import get_key" "get_key()"`.

**NV-4 — `config.yaml` contains JSON.** `config/_globals.py:5` names the file `config.yaml`, but `_config.py`
reads and writes it with `json.load` and `json.dump` (lines 22, 37, 40). JSON is a subset of YAML so nothing
breaks, but the name misleads. The file name is part of the on-disk contract, so renaming it would orphan every
existing user's configuration. **Question:** leave as is, or rename with a migration? Recommend leaving it and
adding a comment.

**NV-5 — `beartype` is a hard dependency applied to two functions.** `generate_archive_summaries` and
`generate_archive_totals` carry `@beartype.beartype`; `generate_summaries`, `generate_all_dataset_totals`, and
every other public function do not. Either extending or removing it changes runtime behavior. **Question:** is
the intent to decorate all public entry points? Confirmed with `grep -rn "beartype" src/`, which returns four
lines across two files.

**NV-6 — Test IP addresses that are not bogons.** AGENTS.md says "Never make up IPs to use the testing suite;
always use bogon types." The suite correctly uses RFC 5737 documentation ranges (`192.0.2.x`, `198.51.100.x`,
`203.0.113.x`) and RFC 1918 space in most places, but also uses real routable addresses including `1.1.1.1`,
`8.8.8.8`, `9.9.9.9`, `1.2.3.4`, `2.2.2.2` through `7.7.7.7`, `11.11.11.11`, `13.14.15.16`, `17.18.19.20`, and
`21.22.23.24`. Some real addresses are *necessary*: `52.x` and `192.30.253.1` must fall inside the published AWS
and GitHub ranges for the service-matching tests to mean anything. The rest look like filler. Swapping them is
**not** behavior-neutral for the tests, since a bogon resolves to `"bogon"` rather than to a country, so the
expectations would need reworking. **Question:** which of these are deliberate? Enumerate with
`grep -rhoE '"[0-9]{1,3}(\.[0-9]{1,3}){3}"' tests/ | sort -u`.

**NV-7 — `BaseValidator.__hash__` and the abstract `_run_validation` body never execute.** All five concrete
validators override `__hash__`, and `BaseValidator` is an `abc.ABC` with an abstract method, so it cannot be
instantiated; its `__hash__` body (lines 16-19) is therefore unreachable within this repository. The same applies
to the `NotImplementedError` on lines 49-50, since no subclass calls `super()._run_validation()`. **Both are
nonetheless public extension surface**: `BaseValidator` is exported in `validate.__all__`, so a downstream
subclass that does not override `__hash__` relies on it. Do not remove either. Recorded here so that a future
reviewer does not mistake them for dead code. Confirmed with `grep -rn "super()" src/`, which shows only
`super().__init__()` calls.

**NV-8 — The broader extractor inheritance question.** Beyond DUP-2 and DUP-12, `RemoteS3LogAccessExtractor`
duplicates the first seven lines of `S3LogAccessExtractor.__init__` verbatim, the record-corruption check with
an identical message, and the batch-dispatch loop. A shared base class would remove roughly 60 further lines, but
it **cannot be proven behavior-neutral by review**, for four reasons. The two `__init__` signatures differ, one
being keyword-only (`*, cache_directory=...`) and the other not, and both are public. The corruption check runs
eagerly in the local extractor's `__init__` and lazily inside `_get_unprocessed_s3_urls` in the remote one, so
the timing of the `ValueError` differs. The local extractor creates an extra `mkdtemp` and reassigns
`_awk_env["EXTRACTION_DIRECTORY"]` before its batch loop (lines 110-112) where the remote does not. And the
remote recreates its temporary directory after each batch (lines 185-186) where the local does not. **Question:**
worth a dedicated pull request with its own test plan? Recommend yes, but separately from this review.

**NV-9 — `s3fs` and `s5cmd` are dependencies with no import.** `s3fs` appears in the `remote` extra but is never
imported; it is nonetheless required, because `fsspec.filesystem("s3")` and `fsspec.open("s3://...")` need it as
the backend. `s5cmd` is also declared there but is invoked as a shell binary through `_deploy_subprocess`
(four call sites, lines 287-329). Both are legitimate runtime-only dependencies. **Recorded so that nobody
removes them as unused.** Verify with
`python -c "import fsspec; print(fsspec.filesystem('s3'))"` in an environment without `s3fs`, which fails.

**NV-10 — The `--workers` help text is truncated.** Lines 52-56 and 336-340 both end with `"By default, "`,
breaking off mid-sentence. This is a real documentation defect, but the text is `--help` output and therefore part
of the CLI surface, so correcting it is out of scope for a behavior-neutral review. **Question:** what should the
sentence say? Presumably "By default, all but one available core is used."

**NV-11 — Validators cannot use a non-default cache directory.** `BaseValidator.__init__` calls
`get_cache_subdirectory(name="records")` (line 22) with no `cache_directory` argument, so the five pre-validators
always write their records to the configured default even though almost every other entry point accepts a
`--cache` override. `RemoteS3BucketValidator.__init__` does accept `cache_directory`. **Question:** is this an
intentional asymmetry? Adding the parameter would change where files are written.

**NV-12 — Three pre-validators have no dedicated test module.** There are test files for `downloads_logic` and
`extraction_heuristic`, but none for `http_empty_split`, `http_split_count`, or `timestamps_parsing`. Recorded as
a coverage gap only. Writing tests is outside the scope of a functionality-preserving review, and any new tests
would need the `ai_generated` marker per AGENTS.md. Confirm with `ls tests/test_*pre_validator*.py`.

---

## Recommended Order of Changes

Each group is independently applicable and testable as one commit. The ordering puts mechanically verifiable
changes first so that the structural refactors later land on a codebase whose noise has already been removed.

Per AGENTS.md, every group that touches `src/` needs: `pre-commit` run before committing, an entry in the
`## Upcoming` section of `CHANGELOG.md` under `### 🏠 Internal`, and a single bump of `version` in
`pyproject.toml` for the pull request as a whole (once, not once per commit).

**Group 1 — Linter-verifiable mechanical cleanup.** CL-1, CL-2, CL-9, CL-18, DC-4, CON-3.
These belong together because every one is flagged by a ruff rule, so the whole group is verified by re-running
ruff with those rules selected and seeing it come back clean. No judgement calls.
Verification: `ruff check --select RET504,C416,C420,SIM118,PIE790 src/ tests/` returns clean, then
`python -m pytest tests/ -m "not remote" -q` (expect 175 passed, 3 deselected).

**Group 2 — Dead code removal.** DC-1, DC-2, DC-5.
Grouped because each is a deletion justified by a reachability argument rather than a linter, so they want the
same kind of review attention. Keep them out of Group 1 so that a reviewer who disagrees with one reachability
argument can revert this group alone.
Verification: `grep -rn "_ip_in_cidr" src/ tests/` returns nothing, then `python -m pytest tests/ -m "not remote" -q`.

**Group 3 — Annotation and naming corrections.** CL-3, CL-4, CL-6, CL-7, CL-8, CL-10, CL-11, CL-12, CL-15, CON-7.
All are local, non-structural, and individually obvious. None changes a runtime value. Grouped so the diff is
easy to scan in one pass.
Verification: `python -m pytest tests/ -m "not remote" -q`, plus
`python -c "from s3_log_extraction.extractors import get_running_pids; print(type(get_running_pids()))"` prints
`<class 'set'>`.

**Group 4 — Configuration hygiene.** DC-3, and DC-6 as a *separate commit within the group*.
DC-3 is proven by running ruff. DC-6 touches the build backend and needs a real `python -m build` to verify, so
it must be its own commit and could reasonably be deferred entirely.
Verification: `ruff check .` reports "All checks passed!" and `pre-commit run --all-files` passes. For DC-6,
compare the `Version:` field of `*.dist-info/METADATA` in wheels built before and after.

**Group 5 — AGENTS.md compliance.** CON-1, CON-2.
Both are import relocations with no logic change, and both were verified by experiment rather than inspection.
Verification: `python -c "import s3_log_extraction"` succeeds, then
`python -m pytest tests/ -m "not remote" -q`.

**Group 6 — `summarize/` deduplication.** DUP-1, DUP-6, DUP-7, then CL-16 as a consequence of DUP-6.
These four all land in `summarize/` and all reduce to "extract one helper, update its call sites". Doing them
together avoids three separate rounds of touching the same four files. DUP-1 first, since it is the most
mechanical.
Verification: `python -m pytest tests/test_generic_summaries.py -q` (the largest test module, 1,111 lines, with
golden-file comparison against `tests/expected_output/`).

**Group 7 — `validate/` deduplication.** DUP-3, then DUP-4.
DUP-3 first and separately, because the hash value determines record file names and so deserves its own
verification step. DUP-4 second, and it carries the `env` caveat that must not be got wrong.
Verification: before and after,
`python -c "from s3_log_extraction.validate import DownloadsLogicPreValidator as V; print(hex(hash(V()))[2:])"`
must print the same string. Then
`python -m pytest tests/test_downloads_logic_pre_validator.py tests/test_extraction_heuristic_pre_validator.py -q`.

**Group 8 — `utils/inventory.py` deduplication.** DUP-5, CON-5, CL-13.
All three touch the same 500-line module, and DUP-5 is the largest single consolidation win in the codebase.
Verification: `python -m pytest tests/test_log_bucket_stats.py tests/test_remote_extractor_inventory.py -q`
(about 90 tests).

**Group 9 — CLI deduplication.** DUP-8, DUP-15, then DUP-9.
DUP-8 and DUP-15 are internal to the function bodies and cannot affect `--help`. DUP-9 changes how options are
*declared*, so it is the one item in this review with a real risk of altering help output, and it goes last and
alone.
Verification: capture `--help` for every command and subcommand before the group and diff after, requiring a
byte-identical file. Then `python -m pytest tests/test_cli_integration.py tests/test_log_bucket_stats.py -q`.

**Group 10 — `extractors/` deduplication.** DUP-2, DUP-12.
The two provable extractions only. The broader inheritance refactor (NV-8) is explicitly *not* in this group.
Verification: `python -m pytest tests/test_generic_extraction.py tests/test_cli_integration.py -q`, which runs
real `gawk` subprocesses in both serial and parallel modes.

**Group 11 — Structural refactors.** DUP-10, DUP-11, CON-4, CON-6.
These reshape control flow rather than lifting identical blocks, so each needs its own careful read. Last,
because by this point the duplication that obscures them has already gone.
Verification: `python -m pytest tests/ -m "not remote" -q` in full, and diff the generated summary tree against
`tests/expected_output/` directly.

**Group 12 — Test-suite cleanup.** DUP-13, DUP-14, CL-18 (the test occurrences).
Last because it touches no production code and can land independently of everything above. No version bump and
no `CHANGELOG.md` entry are needed for a `tests/`-only change, per the AGENTS.md bump rule.
Verification: `python -m pytest tests/ -m "not remote" -q` must report the same count, 175 passed.

**Deliberately not scheduled:** CL-5 (the `config/_config.py` site needs a maintainer's sign-off), CL-14 and
CL-17 (worth doing but each needs a human eyeball on progress output and on a float comparison respectively), and
every NV item.

---

## Not Changed (Deliberately)

These look like findings and are not. Each was considered and rejected.

- **`BaseValidator.__hash__` and the abstract `_run_validation` body.** Unreachable within this repository, but
  `BaseValidator` is public extension surface. See NV-7.
- **`_hidden_top_level_imports.py`.** Seven imports whose only purpose is the side effect of loading submodules,
  with `_hide = True` as a dummy binding. It looks like dead code, is documented as intentional, and has its own
  `F401` exemption in `pyproject.toml`. Leave it.
- **Leading-underscore names in `utils.__all__`.** `_handle_max_workers` and `_read_s3_urls_from_local_inventory`
  are exported despite AGENTS.md saying never to expose private names in `__all__`. They are imported by name
  elsewhere in `src/`, so they are part of the public surface in practice. Fixing this is a breaking change.
- **`list()` around the cached range list in `fetch_service_networks`** (`_resolver.py:49`). It looks like a
  redundant conversion of a value that is already a list, but `_get_cidr_address_ranges_and_subregions` is
  `lru_cache`d, so the copy is what stops a caller from mutating the cached object. Load-bearing.
- **The tuple rebuilding in `_freeze_service_networks`** (`_resolver.py:59`). `tuple((a, b) for a, b in networks)`
  looks like it could be `tuple(networks)`, but `service_networks` is a caller-supplied argument of a public
  constructor, so the inner elements may arrive as lists. The rebuild normalizes them to tuples, without which
  `_build_prefix_tables`'s `lru_cache` would raise on an unhashable argument. Load-bearing.
- **`MappingRegionResolver.resolve`'s `.get(ip) or "missing"`** (`_resolver.py:258`). Not equivalent to
  `.get(ip, "missing")`: the current form also maps an empty-string label to `"missing"`. Changing it is a
  behavior change.
- **`self._owns_reader = True` inside `_get_geolite2_reader`** (`_resolver.py:170`). Looks redundant given the
  constructor, but a resolver constructed with a borrowed reader, then closed, then used again opens its own
  reader and must then own it. Load-bearing.
- **`extraction_directory.mkdir` before `shutil.rmtree`** in `reset_extraction` (`_reset.py:22-24`). The first
  `mkdir` exists so `rmtree` does not raise on a missing directory.
- **`_deploy_subprocess` versus the validators' inline `subprocess.run`.** These look like the same plumbing
  duplicated across modules, but they are not interchangeable: `_deploy_subprocess` merges `environment_variables`
  into `os.environ`, sets `encoding="utf-8"`, and raises a differently formatted `RuntimeError`. The validators
  depend on `env` *replacing* the environment and on their own message format. Merging them would change both the
  environment the awk scripts see and the text of every validation failure.
- **`environment_variables` replacing rather than extending the environment** in
  `ExtractionHeuristicPreValidator` (line 128). It looks like a bug, since the awk subprocess loses `PATH`, but
  the command resolves through `/bin/sh`'s default `PATH` and the awk script reads its one parameter from
  `ENVIRON`. Working as built.
- **Five copies of `# TODO: parallelize` and `# TODO: does this hold after bundling?`** across `validate/`.
  Deliberate per-class markers, not duplication to collapse.
- **Anything under `tests/expected_output/`.** Golden files that define correctness.
- **The `missing`, `undetermined`, and `None` region-label paths**, and `_read_summary_value`'s handling of
  `"<50"` sentinels. These look like orphans of the removed `ip_to_region.yaml` cache, but `CHANGELOG.md`
  documents them as deliberately retained so that summaries written by earlier versions still read correctly.
  The docstrings at `_globals.py:12-16` and `_generate_summaries.py:29-31` say so explicitly.
