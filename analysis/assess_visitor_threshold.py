"""
Assess whether a per-IP activity threshold for counting "visitors" is defensible.

Motivation
----------
The `number_of_requesters` / visitor counts include every unique IP, even those
that made a single drive-by request (crawlers, probes, one-off previews). We may
want to report a curated "engaged visitor" count that excludes trivial IPs, while
still reporting the total unique-IP count separately.

The session-timeout analysis (``assess_streaming_sessions.py``) found a natural
threshold because inter-request gaps are *bimodal* (within-session bursts vs.
between-session breaks) with a real dead zone between. Per-IP activity is very
likely NOT bimodal — it is a heavy-tailed power law — so this script is written to
*test that assumption* rather than assume a valley exists:

  * histogram + CCDF (log-log) of each per-IP metric — a straight CCDF line means a
    pure power law with NO valley and an arbitrary cutoff;
  * a minimum-density valley finder (as in the session analysis) AND a knee finder
    (max-curvature / Kneedle-style), since a power law has a knee, not a valley;
  * a visitor-count-vs-threshold sweep showing how many unique IPs survive each
    cutoff K and what fraction of total activity they account for — the practical
    decision aid.

Per-IP metrics computed (a visitor can plausibly be defined on any of these):
  * total_requests     — all requests (streaming + downloads)
  * streaming_requests — HTTP 206 only (download == 0)
  * n_sessions         — streaming sessions with an 8 h inactivity gap (the shipped
                         `number_of_views` definition, aggregated per IP)
  * distinct_assets    — number of distinct assets the IP touched

Note the mechanism-based floor from issue #74: genuinely streaming a file needs a
HEAD + several range GETs, so an IP with a single request never "viewed" anything.
Defining a visitor as an IP with >= 1 streaming session is principled and needs no
valley — the sweep quantifies how that compares to raw request-count cutoffs.

Usage
-----
    python assess_visitor_threshold.py --cache-dir /path/to/cache \\
        [--dataset DANDI:000123] [--no-encryption] \\
        [--exclude-asset-file testing_blobs.txt] [--out visitor_threshold.png]

Requires: numpy, pandas, matplotlib, tqdm. Encryption password via
S3_LOG_EXTRACTION_PASSWORD if the cache is encrypted (the default).
"""

import argparse
import fnmatch
import pathlib
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm

TIMESTAMP_FORMAT = "%y%m%d%H%M%S"
SESSION_TIMEOUT_SECONDS = 8 * 3600


def _read_lines(path: pathlib.Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def _read_ips(path: pathlib.Path, use_encryption: bool) -> list[str]:
    if not use_encryption:
        return _read_lines(path)
    sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))
    from s3_log_extraction.utils.encryption import read_text_from_file

    text = read_text_from_file(file_path=path, use_encryption=True)
    return [line.strip() for line in text.splitlines() if line.strip()]


def load_per_ip_metrics(
    cache_dir: pathlib.Path,
    dataset_filter: str | None,
    use_encryption: bool,
    exclude_patterns: list[str] | None,
) -> pd.DataFrame:
    """
    Walk the extraction cache and return a per-IP DataFrame with columns
    ``total_requests``, ``streaming_requests``, ``n_sessions``, ``distinct_assets``.
    """
    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory found under {cache_dir}")
    exclude_patterns = exclude_patterns or []

    asset_dirs = []
    for dataset_dir in sorted(extraction_root.iterdir()):
        if not dataset_dir.is_dir():
            continue
        if dataset_filter and dataset_filter not in dataset_dir.name:
            continue
        for asset_dir in dataset_dir.rglob("*"):
            if (asset_dir / "timestamps.txt").exists():
                asset_dirs.append(asset_dir)
    if not asset_dirs:
        raise ValueError(f"No asset directories found under {extraction_root}")
    print(f"Found {len(asset_dirs)} asset directories")

    # Per-IP accumulators.
    total_requests: dict[str, int] = {}
    streaming_requests: dict[str, int] = {}
    distinct_assets: dict[str, set] = {}
    # For sessions we need the per-(IP, asset) streaming timestamps; accumulate then reduce.
    n_sessions: dict[str, int] = {}

    n_excluded = 0
    for asset_index, asset_dir in enumerate(tqdm.tqdm(asset_dirs, desc="Loading assets")):
        asset_path = str(asset_dir.relative_to(extraction_root))
        if any(fnmatch.fnmatch(asset_path, pat) for pat in exclude_patterns):
            n_excluded += 1
            continue

        downloads_path = asset_dir / "download.txt"
        ips_path = asset_dir / "ips.txt"
        timestamps_path = asset_dir / "timestamps.txt"
        if not (downloads_path.exists() and ips_path.exists()):
            continue

        timestamps_raw = _read_lines(timestamps_path)
        downloads_raw = _read_lines(downloads_path)
        ips_raw = _read_ips(ips_path, use_encryption)
        if not (len(timestamps_raw) == len(downloads_raw) == len(ips_raw)):
            print(f"  Warning: mismatched line counts in {asset_dir}, skipping")
            continue

        # Per-asset, group this asset's streaming timestamps by IP to count sessions.
        streaming_ts_by_ip: dict[str, list[int]] = {}
        for ts_str, dl_str, ip in zip(timestamps_raw, downloads_raw, ips_raw):
            total_requests[ip] = total_requests.get(ip, 0) + 1
            distinct_assets.setdefault(ip, set()).add(asset_index)
            if dl_str == "0":
                streaming_requests[ip] = streaming_requests.get(ip, 0) + 1
                # Parse to epoch seconds for session gap computation.
                ts = pd.to_datetime(ts_str, format=TIMESTAMP_FORMAT, errors="coerce")
                if ts is not pd.NaT:
                    streaming_ts_by_ip.setdefault(ip, []).append(int(ts.value // 10**9))

        for ip, epochs in streaming_ts_by_ip.items():
            epochs.sort()
            sessions = 1 + sum(1 for a, b in zip(epochs, epochs[1:]) if (b - a) > SESSION_TIMEOUT_SECONDS)
            n_sessions[ip] = n_sessions.get(ip, 0) + sessions

    if n_excluded:
        print(f"Excluded {n_excluded} asset(s) via patterns {exclude_patterns}")

    ips = sorted(total_requests)
    df = pd.DataFrame(
        {
            "ip": ips,
            "total_requests": [total_requests[ip] for ip in ips],
            "streaming_requests": [streaming_requests.get(ip, 0) for ip in ips],
            "n_sessions": [n_sessions.get(ip, 0) for ip in ips],
            "distinct_assets": [len(distinct_assets[ip]) for ip in ips],
        }
    )
    print(f"Aggregated {len(df):,} unique IPs")
    return df


def find_min_density_valley(values: np.ndarray, n_bins: int = 60) -> dict | None:
    """Lowest-density log-spaced bin over the positive values (as in the session analysis)."""
    v = values[values > 0]
    if v.size < n_bins:
        return None
    edges = np.logspace(np.log10(v.min()), np.log10(v.max()), n_bins + 1)
    counts, _ = np.histogram(v, bins=edges)
    density = counts / np.diff(np.log10(edges))
    i = int(np.argmin(density))
    return {"lower": float(edges[i]), "upper": float(edges[i + 1]), "count": int(counts[i])}


def find_knee(values: np.ndarray) -> float | None:
    """
    Kneedle-style knee of the CCDF on log-log axes: the x (threshold) of maximum
    distance below the straight chord joining the endpoints. For a pure power law
    the CCDF is a straight line and the knee is weak/ill-defined (near an endpoint).
    """
    v = np.sort(values[values > 0])
    if v.size < 10:
        return None
    x = np.log10(v)
    ccdf = 1.0 - np.arange(v.size) / v.size
    y = np.log10(ccdf)
    x0, x1, y0, y1 = x[0], x[-1], y[0], y[-1]
    if x1 == x0:
        return None
    chord = y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    distance = chord - y  # positive where CCDF bows below the chord
    return float(10 ** x[int(np.argmax(distance))])


def _fmt(x: float) -> str:
    return f"{x:,.0f}" if x >= 1 else f"{x:.2g}"


def plot_and_report(df: pd.DataFrame, out_path: pathlib.Path) -> None:
    metrics = [
        ("total_requests", "total requests / IP"),
        ("streaming_requests", "streaming requests / IP"),
        ("n_sessions", "streaming sessions / IP"),
        ("distinct_assets", "distinct assets / IP"),
    ]
    fig, axes = plt.subplots(2, len(metrics), figsize=(5 * len(metrics), 9))
    fig.suptitle("Per-IP activity: is a 'visitor' threshold defensible?", fontsize=14, fontweight="bold")

    for col, (key, label) in enumerate(metrics):
        values = df[key].to_numpy(float)
        positive = values[values > 0]

        # Row 0: histogram of log10(value)
        ax = axes[0, col]
        if positive.size:
            ax.hist(np.log10(positive), bins=60, color="steelblue", edgecolor="none", alpha=0.85)
        ax.set_xlabel(f"{label} (log₁₀)")
        ax.set_ylabel("number of IPs")
        ax.set_title(f"Distribution of {label}", fontsize=9)
        valley = find_min_density_valley(values)
        if valley:
            centre = np.log10((valley["lower"] * valley["upper"]) ** 0.5)
            ax.axvline(centre, color="tomato", ls="--", lw=1, label=f"min-density bin ≈{_fmt(10**centre)}")
            ax.legend(fontsize=7)

        # Row 1: CCDF log-log + knee
        ax = axes[1, col]
        if positive.size:
            sv = np.sort(positive)
            ccdf = 1.0 - np.arange(sv.size) / sv.size
            ax.loglog(sv, ccdf, color="steelblue", lw=1.5)
        knee = find_knee(values)
        if knee:
            ax.axvline(knee, color="darkorange", ls="--", lw=1.2, label=f"knee ≈ {_fmt(knee)}")
            ax.legend(fontsize=7)
        ax.set_xlabel(label)
        ax.set_ylabel("fraction of IPs ≥ x (CCDF)")
        ax.set_title(f"CCDF of {label}\n(straight line ⇒ power law, no valley)", fontsize=9)
        ax.grid(True, which="both", alpha=0.2)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def print_sweep(df: pd.DataFrame) -> None:
    """Visitor-count-vs-threshold sweep for each metric."""
    metrics = ["total_requests", "streaming_requests", "n_sessions", "distinct_assets"]
    total_ips = len(df)
    for key in metrics:
        values = df[key].to_numpy(float)
        total_activity = values.sum()
        print(f"\n--- Threshold sweep on {key} (total unique IPs = {total_ips:,}) ---")
        print(f"  {'K (>= )':>8} {'IPs kept':>12} {'% IPs':>7} {'% activity kept':>16}")
        for k in [1, 2, 3, 5, 10, 20, 50, 100]:
            kept = int((values >= k).sum())
            activity = float(values[values >= k].sum())
            pct_ips = 100 * kept / total_ips if total_ips else 0.0
            pct_act = 100 * activity / total_activity if total_activity else 0.0
            print(f"  {k:>8} {kept:>12,} {pct_ips:>6.1f}% {pct_act:>15.2f}%")
        one_offs = int((values == 1).sum())
        print(f"  IPs with exactly 1: {one_offs:,} ({100 * one_offs / total_ips:.1f}% of IPs)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--dataset", default=None, help="Optional dataset-name substring filter")
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument("--exclude-asset-file", type=pathlib.Path, default=None, help="One exclusion glob per line")
    parser.add_argument("--out", default="visitor_threshold.png", type=pathlib.Path)
    args = parser.parse_args()

    exclude_patterns: list[str] = []
    if args.exclude_asset_file:
        exclude_patterns = [
            line.strip()
            for line in args.exclude_asset_file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    df = load_per_ip_metrics(
        cache_dir=args.cache_dir,
        dataset_filter=args.dataset,
        use_encryption=not args.no_encryption,
        exclude_patterns=exclude_patterns,
    )
    plot_and_report(df, out_path=args.out)
    print_sweep(df)


if __name__ == "__main__":
    main()
