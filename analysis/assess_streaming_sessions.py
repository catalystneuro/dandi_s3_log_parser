"""
Session separability assessment for streaming requests.

Loads all streaming (HTTP 206, download=0) requests from the extraction cache,
computes inter-request intervals per IP, and produces two plots:

1. Empirical CDF + histogram of inter-request intervals on a log time axis
   (bin-size-agnostic — reveals natural bimodality if present).

2. For each candidate session bin size (10 min, 30 min, 1 hr, 2 hr, 4 hr),
   the distribution of "gap-to-next-request-beyond-the-bin" across all IPs/assets.
   A clean gap distribution (mass concentrated at large values) supports using
   that bin as a session timeout.

Usage
-----
    python assess_streaming_sessions.py --cache-dir /path/to/extraction/cache \
        [--dataset DANDI:000123] [--no-encryption] [--out session_assessment.png]

The script requires: numpy, pandas, matplotlib, tqdm.
If IPs are encrypted, the encryption password must be available via the
S3_LOG_EXTRACTION_PASS environment variable (same as the main library).
"""

import argparse
import fnmatch
import pathlib
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm

TIMESTAMP_FORMAT = "%y%m%d%H%M%S"  # YYMMDDHHmmss


def _parse_timestamp(ts_str: str) -> pd.Timestamp:
    return pd.to_datetime(ts_str.strip(), format=TIMESTAMP_FORMAT)


def _read_lines(path: pathlib.Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def _read_ips(path: pathlib.Path, use_encryption: bool) -> list[str]:
    if not use_encryption:
        return _read_lines(path)
    # Reuse the library's decryption helper
    sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))
    from s3_log_extraction.utils.encryption import read_text_from_file

    text = read_text_from_file(file_path=path, use_encryption=True)
    return [line.strip() for line in text.splitlines() if line.strip()]


def load_streaming_requests(
    cache_dir: pathlib.Path,
    dataset_filter: str | None,
    use_encryption: bool,
    exclude_patterns: list[str] | None = None,
) -> pd.DataFrame:
    """Walk the extraction cache and collect all streaming (download=0) requests."""
    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory found under {cache_dir}")

    exclude_patterns = exclude_patterns or []

    # Gather all asset directories (leaf dirs containing timestamps.txt)
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
        raise ValueError(f"No asset directories with timestamps.txt found under {extraction_root}")

    print(f"Found {len(asset_dirs)} asset directories")

    n_excluded_assets = 0
    records = []
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Loading assets"):
        # Asset path relative to the extraction root, e.g. "ds000123/sub-01/.../file.nwb"
        asset_path = str(asset_dir.relative_to(extraction_root))
        if any(fnmatch.fnmatch(asset_path, pat) for pat in exclude_patterns):
            n_excluded_assets += 1
            continue

        timestamps_path = asset_dir / "timestamps.txt"
        downloads_path = asset_dir / "download.txt"
        ips_path = asset_dir / "ips.txt"

        if not (downloads_path.exists() and ips_path.exists()):
            continue

        timestamps_raw = _read_lines(timestamps_path)
        downloads_raw = _read_lines(downloads_path)
        ips_raw = _read_ips(ips_path, use_encryption)

        n = len(timestamps_raw)
        if not (len(downloads_raw) == n == len(ips_raw)):
            print(f"  Warning: mismatched line counts in {asset_dir}, skipping")
            continue

        for ts_str, dl_str, ip in zip(timestamps_raw, downloads_raw, ips_raw):
            if dl_str == "0":  # streaming only
                try:
                    ts = _parse_timestamp(ts_str)
                    records.append({"timestamp": ts, "ip": ip, "asset_path": asset_path})
                except ValueError:
                    pass

    if n_excluded_assets:
        print(f"Excluded {n_excluded_assets} asset(s) matching {exclude_patterns}")

    if not records:
        raise ValueError("No streaming requests found — check cache path and filters")

    df = pd.DataFrame(records)
    df.sort_values("timestamp", inplace=True)
    print(f"Loaded {len(df):,} streaming requests from {df['ip'].nunique():,} unique IPs")
    return df


def compute_inter_request_intervals(df: pd.DataFrame, per_asset: bool = False) -> pd.DataFrame:
    """
    Return a tidy DataFrame of inter-request intervals.

    Columns: ``ip``, ``interval`` (seconds), and — when the input carries an
    ``asset_path`` column — ``asset_before`` / ``asset_after`` identifying the
    assets of the two requests that bound each gap. The asset attribution lets
    downstream code attribute "moat-filling" intervals to specific assets (e.g.
    bot-hammered testing assets).

    By default gaps are between consecutive requests from the same IP *across all
    assets* (the calibration used for the archive-wide session valley). With
    ``per_asset=True`` gaps are computed within each (IP, asset) group instead, i.e.
    the delay between successive touches of the *same* asset by the same IP — the
    scope the shipped per-asset ``number_of_views`` metric actually applies, and the
    right distribution to calibrate that metric's timeout on. Note this is NOT the
    same as filtering ``asset_before == asset_after``: same-asset returns with other
    assets touched in between are only captured by the (IP, asset) grouping.
    """
    has_asset = "asset_path" in df.columns
    if per_asset and not has_asset:
        raise ValueError("per_asset=True requires an 'asset_path' column")
    seg_ip, seg_int, seg_before, seg_after = [], [], [], []
    group_keys = ["ip", "asset_path"] if per_asset else "ip"
    groups = list(df.groupby(group_keys))
    for key, group in tqdm.tqdm(groups, desc="Computing intervals", unit="grp"):
        ip = key[0] if per_asset else key
        g = group.sort_values("timestamp")
        times = g["timestamp"].to_numpy()
        if times.size < 2:
            continue
        deltas = (np.diff(times) / np.timedelta64(1, "s")).astype(np.float64)
        mask = deltas > 0
        if not mask.any():
            continue
        seg_int.append(deltas[mask])
        seg_ip.append(np.full(int(mask.sum()), ip, dtype=object))
        if has_asset:
            assets = g["asset_path"].to_numpy()
            seg_before.append(assets[:-1][mask])
            seg_after.append(assets[1:][mask])

    if not seg_int:
        return pd.DataFrame({"ip": [], "interval": []})

    out = pd.DataFrame({"ip": np.concatenate(seg_ip), "interval": np.concatenate(seg_int)})
    if has_asset:
        out["asset_before"] = np.concatenate(seg_before)
        out["asset_after"] = np.concatenate(seg_after)
    return out


def attribute_band_intervals(intervals_df: pd.DataFrame, lo_seconds: float, hi_seconds: float, top_n: int = 10) -> dict:
    """
    Attribute the intervals falling within ``[lo_seconds, hi_seconds]`` to assets and IPs.

    These are the gaps that would land in a session "moat" and thus threaten
    determinism. If they concentrate on a few assets (and are mostly same-asset,
    the bot signature), those are the testing assets to exclude.
    """
    band = intervals_df[(intervals_df["interval"] >= lo_seconds) & (intervals_df["interval"] <= hi_seconds)]
    result = {"n_in_band": int(len(band)), "top_assets": None, "top_ips": None, "same_asset_fraction": None}
    if len(band) == 0 or "asset_after" not in band.columns:
        return result

    same_asset = (band["asset_before"] == band["asset_after"]).mean()
    result["same_asset_fraction"] = float(same_asset)
    result["top_assets"] = band["asset_after"].value_counts().head(top_n)
    result["top_ips"] = band["ip"].value_counts().head(top_n)
    return result


def compute_gap_beyond_bin(df: pd.DataFrame, bin_seconds: int, label: str = "") -> np.ndarray:
    """
    For each IP, find requests that would be the *last* in a time bin,
    then measure how long until the *next* request from that IP.
    Returns array of those gap durations in seconds (NaN = no next request).
    """
    gaps = []
    groups = list(df.groupby("ip"))
    for _ip, group in tqdm.tqdm(groups, desc=f"Gap-beyond-bin ({label})", unit="IP"):
        times = group["timestamp"].sort_values().reset_index(drop=True)
        ts_sec = times.astype(np.int64) // 10**9  # epoch seconds

        i = 0
        while i < len(ts_sec):
            bin_start = ts_sec.iloc[i]
            bin_end = bin_start + bin_seconds
            # find last request within this bin
            in_bin = ts_sec[ts_sec < bin_end]
            last_in_bin_idx = in_bin[in_bin.index >= i].index.max() if not in_bin[in_bin.index >= i].empty else i
            # find next request after the bin
            after_bin = ts_sec[ts_sec >= bin_end]
            if after_bin.empty:
                break
            next_after_bin_idx = after_bin.index.min()
            gap = ts_sec.iloc[next_after_bin_idx] - ts_sec.iloc[last_in_bin_idx]
            gaps.append(float(gap))
            i = next_after_bin_idx

    return np.array(gaps)


def _fmt_seconds(s: float) -> str:
    """Human-readable duration."""
    if s < 90:
        return f"{s:.0f}s"
    if s < 5400:
        return f"{s / 60:.1f}m"
    if s < 129600:
        return f"{s / 3600:.1f}h"
    return f"{s / 86400:.2f}d"


def find_min_density_band(
    intervals: np.ndarray, lo_seconds: float = 60.0, hi_seconds: float = 2 * 86400.0, n_bins: int = 60
) -> dict | None:
    """
    Find the minimum-density valley of the inter-request interval distribution,
    using log-spaced bins over ``[lo_seconds, hi_seconds]``.

    At realistic data volumes there is no literally empty band — gaps occur at
    every lag — so the best session "moat" is the lowest-density valley rather
    than a zero-count gap. Density is normalized per order of magnitude so the
    comparison is fair across a log axis. Returns the single lowest-density bin
    (edges, geometric-center ``geometric_mid`` as the suggested timeout ``T``,
    its count and density) plus the full per-bin profile (``edges``, ``counts``,
    ``density``) for inspection.

    Returns ``None`` if fewer than ``n_bins`` intervals fall in the region.
    """
    region = intervals[(intervals >= lo_seconds) & (intervals <= hi_seconds)]
    if region.size < n_bins:
        return None

    edges = np.logspace(np.log10(lo_seconds), np.log10(hi_seconds), n_bins + 1)
    counts, _ = np.histogram(region, bins=edges)
    log_widths = np.diff(np.log10(edges))  # width of each bin in orders of magnitude
    density = counts / log_widths  # counts per order of magnitude (fair on a log axis)

    i = int(np.argmin(density))
    return {
        "lower": float(edges[i]),
        "upper": float(edges[i + 1]),
        "geometric_mid": float(np.sqrt(edges[i] * edges[i + 1])),
        "count": int(counts[i]),
        "density_per_decade": float(density[i]),
        "edges": edges,
        "counts": counts,
        "density": density,
    }


def guard_band_report(intervals: np.ndarray, candidate_seconds: float, rel_tolerance: float = 0.1) -> dict:
    """
    For a candidate session timeout ``T``, count observed intervals falling within
    the multiplicative guard band ``[T / (1 + tol), T * (1 + tol)]`` — the
    "ambiguity count". Zero means the cutoff is perfectly deterministic in this
    snapshot; this same count is the metric to monitor on future data.
    """
    lo = candidate_seconds / (1 + rel_tolerance)
    hi = candidate_seconds * (1 + rel_tolerance)
    in_band = int(np.count_nonzero((intervals >= lo) & (intervals <= hi)))
    return {
        "candidate_seconds": candidate_seconds,
        "guard_lo": lo,
        "guard_hi": hi,
        "ambiguity_count": in_band,
        "ambiguity_fraction": in_band / intervals.size if intervals.size else 0.0,
    }


def sweep_guard_bands(
    intervals: np.ndarray,
    lo_seconds: float = 60.0,
    hi_seconds: float = 7 * 86400.0,
    n: int = 32,
    rel_tolerance: float = 0.1,
) -> list[dict]:
    """
    Run :func:`guard_band_report` across a coarse log-spaced grid of candidate
    timeouts spanning ``[lo_seconds, hi_seconds]``, so the full ambiguity profile
    (and its valley) is visible across every timescale, not just a few candidates.
    """
    grid = np.logspace(np.log10(lo_seconds), np.log10(hi_seconds), n)
    return [guard_band_report(intervals, float(t), rel_tolerance) for t in grid]


def plot_assessment(
    intervals: np.ndarray,
    bin_configs: list[tuple[str, int]],
    df: pd.DataFrame,
    out_path: pathlib.Path,
) -> None:
    n_bins = len(bin_configs)
    fig, axes = plt.subplots(2, n_bins + 1, figsize=(5 * (n_bins + 1), 10))
    fig.suptitle("Streaming Session Separability Assessment", fontsize=14, fontweight="bold")

    # --- Row 0, Col 0: inter-request interval histogram (log x) ---
    ax = axes[0, 0]
    log_intervals = np.log10(intervals[intervals > 0])
    ax.hist(log_intervals, bins=100, color="steelblue", edgecolor="none", alpha=0.8)
    ax.set_xlabel("Inter-request interval (seconds, log₁₀ scale)")
    ax.set_ylabel("Count")
    ax.set_title("Histogram of inter-request intervals\n(all IPs, streaming only)")
    tick_vals = [1, 60, 600, 3600, 86400, 86400 * 7]
    ax.set_xticks([np.log10(v) for v in tick_vals])
    ax.set_xticklabels(["1s", "1m", "10m", "1h", "1d", "1w"], fontsize=8)

    # --- Row 1, Col 0: empirical CDF ---
    ax = axes[1, 0]
    sorted_intervals = np.sort(intervals[intervals > 0])
    cdf = np.arange(1, len(sorted_intervals) + 1) / len(sorted_intervals)
    ax.semilogx(sorted_intervals, cdf, color="steelblue", linewidth=1.5)
    ax.set_xlabel("Inter-request interval (seconds, log scale)")
    ax.set_ylabel("Cumulative fraction")
    ax.set_title("Empirical CDF of inter-request intervals")
    ax.grid(True, alpha=0.3)
    for label, secs in [("10m", 600), ("30m", 1800), ("1h", 3600), ("2h", 7200), ("4h", 14400)]:
        ax.axvline(secs, color="tomato", linestyle="--", linewidth=0.8, alpha=0.7)
        frac = np.searchsorted(sorted_intervals, secs) / len(sorted_intervals)
        ax.text(secs * 1.05, frac, f"{label}\n{frac:.1%}", fontsize=7, color="tomato", va="center")

    # --- Columns 1..N: gap-beyond-bin plots ---
    for col_idx, (label, bin_sec) in enumerate(bin_configs):
        gaps = compute_gap_beyond_bin(df, bin_sec, label=label)
        gaps = gaps[np.isfinite(gaps) & (gaps > 0)]

        # Row 0: histogram
        ax = axes[0, col_idx + 1]
        if len(gaps):
            log_gaps = np.log10(gaps)
            ax.hist(log_gaps, bins=60, color="darkorange", edgecolor="none", alpha=0.8)
        ax.set_xlabel("Gap to next request beyond bin (s, log₁₀)")
        ax.set_ylabel("Count")
        ax.set_title(f"Gap-beyond-bin: {label} window\n(n={len(gaps):,} bin boundaries)")
        tick_vals = [1, 60, 600, 3600, 86400]
        ax.set_xticks([np.log10(v) for v in tick_vals])
        ax.set_xticklabels(["1s", "1m", "10m", "1h", "1d"], fontsize=8)
        ax.axvline(np.log10(bin_sec), color="steelblue", linestyle="--", linewidth=1, label="bin size")
        ax.legend(fontsize=7)

        # Row 1: CDF
        ax = axes[1, col_idx + 1]
        if len(gaps):
            sorted_gaps = np.sort(gaps)
            cdf = np.arange(1, len(sorted_gaps) + 1) / len(sorted_gaps)
            ax.semilogx(sorted_gaps, cdf, color="darkorange", linewidth=1.5)
            ax.axvline(bin_sec, color="steelblue", linestyle="--", linewidth=1, label="bin size")
            ax.legend(fontsize=7)
        ax.set_xlabel("Gap to next request (seconds, log scale)")
        ax.set_ylabel("Cumulative fraction")
        ax.set_title(f"CDF of gap-beyond-bin: {label}")
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved assessment plot to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--cache-dir", required=True, type=pathlib.Path, help="Path to the root extraction cache directory"
    )
    parser.add_argument(
        "--dataset", default=None, help="Optional dataset name/ID substring to filter (e.g. 'DANDI:000123')"
    )
    parser.add_argument("--no-encryption", action="store_true", help="Treat ips.txt files as plaintext (no decryption)")
    parser.add_argument(
        "--exclude-asset",
        action="append",
        default=[],
        metavar="GLOB",
        help="Glob pattern (matched against the asset path relative to extraction/) to exclude, "
        "e.g. '*test*' or 'ds000xxx/*'. Repeatable. Use to drop bot-hammered testing assets.",
    )
    parser.add_argument(
        "--exclude-asset-file",
        type=pathlib.Path,
        default=None,
        help="Optional text file with one exclusion glob per line (added to any --exclude-asset patterns).",
    )
    parser.add_argument(
        "--per-asset",
        action="store_true",
        help="Compute gaps within each (IP, asset) group — the delay between successive touches of the "
        "SAME asset by the same IP — instead of across all of an IP's assets. This matches the scope of "
        "the shipped per-asset number_of_views metric; compare its valley against the default cross-asset one.",
    )
    parser.add_argument(
        "--out",
        default="session_assessment.png",
        type=pathlib.Path,
        help="Output PNG path (default: session_assessment.png)",
    )
    args = parser.parse_args()

    use_encryption = not args.no_encryption

    exclude_patterns = list(args.exclude_asset)
    if args.exclude_asset_file:
        exclude_patterns += [
            line.strip()
            for line in args.exclude_asset_file.read_text().splitlines()
            if line.strip() and not line.startswith("#")
        ]

    df = load_streaming_requests(
        cache_dir=args.cache_dir,
        dataset_filter=args.dataset,
        use_encryption=use_encryption,
        exclude_patterns=exclude_patterns,
    )

    scope = "same-asset (per IP, asset)" if args.per_asset else "cross-asset (per IP)"
    print(f"Computing inter-request intervals [{scope}]...")
    intervals_df = compute_inter_request_intervals(df, per_asset=args.per_asset)
    intervals = intervals_df["interval"].to_numpy()
    print(f"  {len(intervals):,} intervals computed")

    bin_configs = [
        ("10 min", 10 * 60),
        ("30 min", 30 * 60),
        ("1 hour", 60 * 60),
        ("2 hours", 2 * 60 * 60),
    ]

    print("Computing gap-beyond-bin for each window size...")
    plot_assessment(intervals=intervals, bin_configs=bin_configs, df=df, out_path=args.out)

    # Print summary statistics
    print("\n--- Inter-request interval summary ---")
    for label, secs in bin_configs:
        pct = 100 * np.mean(intervals <= secs)
        print(f"  Fraction of intervals ≤ {label}: {pct:.1f}%")
    print(f"  Median interval: {np.median(intervals):.0f}s")
    print(f"  95th percentile: {np.percentile(intervals, 95):.0f}s")
    print(f"  99th percentile: {np.percentile(intervals, 99):.0f}s")

    # --- Session-boundary determinism diagnostic ---
    # (1) Locate the lowest-density valley (the best session "moat"). At realistic
    #     data volumes there is no literally empty band, so the valley is what matters.
    print("\n--- Minimum-density valley in inter-request interval distribution ---")
    valley = find_min_density_band(intervals, lo_seconds=60.0, hi_seconds=2 * 86400.0, n_bins=60)
    if valley is None:
        print("  Not enough intervals in the 60s–2d region of interest.")
    else:
        order = np.argsort(valley["density"])
        print("  Lowest-density bins (best separation valleys):")
        for rank, i in enumerate(order[:3], start=1):
            lo, hi = valley["edges"][i], valley["edges"][i + 1]
            print(
                f"    {rank}. {_fmt_seconds(lo)} – {_fmt_seconds(hi)}: "
                f"{int(valley['counts'][i]):,} intervals ({valley['density'][i]:,.0f} per order of magnitude)"
            )
        print(
            f"  → suggested session timeout T = {_fmt_seconds(valley['geometric_mid'])} "
            f"(center of the single lowest-density bin)"
        )

    # (2) Full coarse sweep of guard-band ambiguity across every timescale, with a
    #     visual bar so the valley is obvious end-to-end.
    print("\n--- Guard-band ambiguity sweep (±10% window, log-spaced from 1m to 7d) ---")
    sweep = sweep_guard_bands(intervals, lo_seconds=60.0, hi_seconds=7 * 86400.0, n=32, rel_tolerance=0.1)
    max_count = max((r["ambiguity_count"] for r in sweep), default=0) or 1
    for r in sweep:
        bar = "█" * int(round(40 * r["ambiguity_count"] / max_count))
        flag = " ← DETERMINISTIC (empty)" if r["ambiguity_count"] == 0 else ""
        print(
            f"  T={_fmt_seconds(r['candidate_seconds']):>7}: "
            f"{r['ambiguity_count']:>9,} ({100 * r['ambiguity_fraction']:.4f}%) {bar}{flag}"
        )

    # (3) Named candidate timeouts with asset/IP attribution of the offending intervals:
    #     when a band is non-empty and piles up on a few same-asset (bot-like) testing
    #     assets, exclude those (see --exclude-asset-file) and re-run.
    print("\n--- Named candidate timeouts (±10% window) with bot attribution ---")
    candidates = [
        ("10 min", 600.0),
        ("30 min", 1800.0),
        ("1 hour", 3600.0),
        ("2 hours", 7200.0),
        ("1 day", 86400.0),
    ]
    if valley is not None:
        t_auto = valley["geometric_mid"]
        candidates.append((f"auto-trough ({_fmt_seconds(t_auto)})", t_auto))
    for label, secs in candidates:
        r = guard_band_report(intervals, secs, rel_tolerance=0.1)
        print(
            f"  {label:>24}: {r['ambiguity_count']:>9,} intervals in "
            f"[{_fmt_seconds(r['guard_lo'])}, {_fmt_seconds(r['guard_hi'])}] "
            f"({100 * r['ambiguity_fraction']:.4f}% of all) "
            f"{'← DETERMINISTIC (empty)' if r['ambiguity_count'] == 0 else ''}"
        )
        if r["ambiguity_count"] > 0 and "asset_after" in intervals_df.columns:
            attribution = attribute_band_intervals(intervals_df, r["guard_lo"], r["guard_hi"], top_n=5)
            if attribution["same_asset_fraction"] is not None:
                if args.per_asset:
                    # In per-asset mode every gap is same-asset by construction, so the
                    # same-asset fraction is a tautology (always 100%) and is NOT a bot
                    # signal. Report only which assets pile up in the band.
                    print("      top contributing assets (periodic ones are candidate bots to exclude):")
                else:
                    print(
                        f"      {100 * attribution['same_asset_fraction']:.1f}% are same-asset gaps "
                        f"(bot signature); top contributing assets:"
                    )
                for asset, count in attribution["top_assets"].items():
                    print(f"        {count:>7,}  {asset}")


if __name__ == "__main__":
    main()
