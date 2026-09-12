"""
Visualize the relationship between asset structural complexity and streaming request counts.

Loads:
  1. Asset metadata CSV produced by prefetch_asset_metadata.py
     (size_bytes, asset_type, n_objects, min/max/avg_path_depth per asset)
  2. Streaming (download=0) request counts from the extraction cache

Then plots the messy/contradictory relationship between structural complexity
and how much an asset gets streamed — assets that are structurally simple but
heavily streamed, or large/complex but rarely touched, are the interesting cases.

Usage
-----
    python assess_asset_streaming_complexity.py \\
        --metadata asset_metadata.csv \\
        --cache-dir /path/to/extraction/cache \\
        [--dataset DANDI:000123] \\
        [--no-encryption] \\
        [--out complexity_assessment.png]
"""

import argparse
import pathlib

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm

# ---------------------------------------------------------------------------
# Load extraction cache streaming counts
# ---------------------------------------------------------------------------


def _read_lines(path: pathlib.Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def _read_ips(path: pathlib.Path, use_encryption: bool) -> list[str]:
    if not use_encryption:
        return _read_lines(path)
    import sys

    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
    from s3_log_extraction.utils.encryption import read_text_from_file

    text = read_text_from_file(file_path=path, use_encryption=True)
    return [line.strip() for line in text.splitlines() if line.strip()]


def load_streaming_counts(
    cache_dir: pathlib.Path,
    dataset_filter: str | None,
    use_encryption: bool,
) -> pd.DataFrame:
    """Return a DataFrame with one row per asset: asset_path, n_streaming_requests, n_unique_ips."""
    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory under {cache_dir}")

    records = []
    dataset_dirs = sorted(extraction_root.iterdir())
    for dataset_dir in tqdm.tqdm(dataset_dirs, desc="Scanning datasets"):
        if not dataset_dir.is_dir():
            continue
        if dataset_filter and dataset_filter not in dataset_dir.name:
            continue
        dandiset_id = dataset_dir.name

        for asset_dir in dataset_dir.rglob("*"):
            if not (asset_dir / "timestamps.txt").exists():
                continue
            downloads_path = asset_dir / "download.txt"
            ips_path = asset_dir / "ips.txt"
            if not (downloads_path.exists() and ips_path.exists()):
                continue

            downloads = _read_lines(downloads_path)
            if not downloads:
                continue

            # relative asset path within the dandiset dir
            rel_path = str(asset_dir.relative_to(dataset_dir))

            streaming_mask = [d == "0" for d in downloads]
            n_streaming = sum(streaming_mask)

            if n_streaming == 0:
                continue

            try:
                ips = _read_ips(ips_path, use_encryption)
            except Exception:
                ips = ["?"] * len(downloads)

            streaming_ips = {ip for ip, is_s in zip(ips, streaming_mask) if is_s}
            records.append(
                {
                    "dandiset_id": dandiset_id,
                    "asset_path": rel_path,
                    "n_streaming_requests": n_streaming,
                    "n_unique_ips": len(streaming_ips),
                    "n_total_requests": len(downloads),
                }
            )

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


def _scatter_with_marginals(ax_main, ax_top, ax_right, x, y, c, cmap, norm, label_x, label_y, title):
    """Scatter on ax_main with 1D histograms on marginal axes."""
    sc = ax_main.scatter(x, y, c=c, cmap=cmap, norm=norm, alpha=0.5, s=18, linewidths=0)
    ax_main.set_xlabel(label_x)
    ax_main.set_ylabel(label_y)
    ax_main.set_title(title, fontsize=9)
    ax_main.grid(True, alpha=0.2)

    ax_top.hist(x, bins=40, color="steelblue", alpha=0.7, edgecolor="none")
    ax_top.set_xlim(ax_main.get_xlim())
    ax_top.axis("off")

    ax_right.hist(y, bins=40, orientation="horizontal", color="darkorange", alpha=0.7, edgecolor="none")
    ax_right.set_ylim(ax_main.get_ylim())
    ax_right.axis("off")

    return sc


def plot_complexity_vs_streaming(merged: pd.DataFrame, out_path: pathlib.Path) -> None:
    # Work in log space; drop rows with non-positive values for log columns
    df = merged.copy()
    df = df[df["size_bytes"] > 0]
    df["log_size"] = np.log10(df["size_bytes"])
    df["log_requests"] = np.log10(df["n_streaming_requests"].clip(lower=1))

    has_objects = df["n_objects"].notna() & (df["n_objects"] > 0)
    df_obj = df[has_objects].copy()
    df_obj["log_n_objects"] = np.log10(df_obj["n_objects"])

    type_colors = {"hdf5": "steelblue", "zarr": "darkorange", "other": "gray"}
    type_vals = df["asset_type"].map(type_colors).fillna("gray")

    # --- Figure layout: 3 rows × 3 cols ---
    # Row 0: size vs requests | n_objects vs requests | requests-per-IP vs size
    # Row 1: avg_path_depth vs requests | max_path_depth vs n_objects | (empty)
    # Row 2: residuals vs size | residuals vs path depth | residuals vs n_objects

    fig = plt.figure(figsize=(18, 16))
    fig.suptitle("Asset Structural Complexity vs. Streaming Request Patterns", fontsize=13, fontweight="bold")

    gs_outer = fig.add_gridspec(3, 3, hspace=0.45, wspace=0.35)

    def _make_panel(gs_cell, with_marginals=True):
        if with_marginals:
            inner = gs_cell.subgridspec(2, 2, height_ratios=[1, 3], width_ratios=[3, 1], hspace=0.05, wspace=0.05)
            return fig.add_subplot(inner[1, 0]), fig.add_subplot(inner[0, 0]), fig.add_subplot(inner[1, 1])
        return fig.add_subplot(gs_cell), None, None

    # ---- Panel (0,0): size vs streaming requests ----
    ax, ax_t, ax_r = _make_panel(gs_outer[0, 0])
    ax.scatter(df["log_size"], df["log_requests"], c=type_vals, alpha=0.45, s=16, linewidths=0)
    ax.set_xlabel("Asset size (bytes, log₁₀)")
    ax.set_ylabel("Streaming requests (log₁₀)")
    ax.set_title("Size vs. streaming requests\n(colour = asset type)", fontsize=9)
    ax.grid(True, alpha=0.2)
    _xtick(ax, [1e3, 1e6, 1e9, 1e12], ["1 KB", "1 MB", "1 GB", "1 TB"])
    _ytick(ax, [1, 10, 100, 1000, 10000])
    for label, color in type_colors.items():
        if label in df["asset_type"].values:
            ax.scatter([], [], c=color, label=label, s=20)
    ax.legend(fontsize=7, loc="upper left")
    if ax_t:
        ax_t.hist(df["log_size"], bins=40, color="steelblue", alpha=0.7, edgecolor="none")
        ax_t.set_xlim(ax.get_xlim())
        ax_t.axis("off")
        ax_r.hist(
            df["log_requests"], bins=40, orientation="horizontal", color="darkorange", alpha=0.7, edgecolor="none"
        )
        ax_r.set_ylim(ax.get_ylim())
        ax_r.axis("off")

    # ---- Panel (0,1): n_objects vs streaming requests ----
    ax, ax_t, ax_r = _make_panel(gs_outer[0, 1])
    if len(df_obj):
        c_obj = df_obj["asset_type"].map(type_colors).fillna("gray")
        ax.scatter(df_obj["log_n_objects"], df_obj["log_requests"], c=c_obj, alpha=0.45, s=16, linewidths=0)
    ax.set_xlabel("Internal objects (log₁₀)")
    ax.set_ylabel("Streaming requests (log₁₀)")
    ax.set_title("Internal object count vs. streaming requests", fontsize=9)
    ax.grid(True, alpha=0.2)
    if ax_t and len(df_obj):
        ax_t.hist(df_obj["log_n_objects"], bins=40, color="steelblue", alpha=0.7, edgecolor="none")
        ax_t.set_xlim(ax.get_xlim())
        ax_t.axis("off")
        ax_r.hist(
            df_obj["log_requests"], bins=40, orientation="horizontal", color="darkorange", alpha=0.7, edgecolor="none"
        )
        ax_r.set_ylim(ax.get_ylim())
        ax_r.axis("off")

    # ---- Panel (1,0): avg path depth vs requests ----
    ax, ax_t, ax_r = _make_panel(gs_outer[1, 0])
    df_depth = df[df["avg_path_depth"].notna()]
    if len(df_depth):
        norm = mcolors.LogNorm(vmin=max(1, df_depth["size_bytes"].min()), vmax=df_depth["size_bytes"].max())
        sc = ax.scatter(
            df_depth["avg_path_depth"],
            df_depth["log_requests"],
            c=df_depth["size_bytes"],
            cmap="viridis",
            norm=norm,
            alpha=0.5,
            s=16,
            linewidths=0,
        )
        plt.colorbar(sc, ax=ax, label="Size (bytes)", pad=0.01)
    ax.set_xlabel("Avg internal path depth (number of /)")
    ax.set_ylabel("Streaming requests (log₁₀)")
    ax.set_title("Path depth vs. streaming requests\n(colour = size)", fontsize=9)
    ax.grid(True, alpha=0.2)

    # ---- Panel (1,1): max path depth vs n_objects ----
    ax, ax_t, ax_r = _make_panel(gs_outer[1, 1])
    if len(df_obj) and df_obj["max_path_depth"].notna().any():
        c_obj = df_obj["asset_type"].map(type_colors).fillna("gray")
        ax.scatter(df_obj["max_path_depth"], df_obj["log_n_objects"], c=c_obj, alpha=0.5, s=16, linewidths=0)
    ax.set_xlabel("Max internal path depth")
    ax.set_ylabel("Internal objects (log₁₀)")
    ax.set_title("Path depth vs. object count\n(structural shape of assets)", fontsize=9)
    ax.grid(True, alpha=0.2)

    # ---- Row 2: residual (actual - size-predicted requests) coloured by depth / type ----
    # Fit log_requests ~ log_size (OLS) then plot residuals
    ax_res0 = fig.add_subplot(gs_outer[2, 0])
    ax_res1 = fig.add_subplot(gs_outer[2, 1])
    ax_res2 = fig.add_subplot(gs_outer[2, 2])

    df_fit = df.dropna(subset=["log_size", "log_requests"])
    if len(df_fit) > 5:
        coeffs = np.polyfit(df_fit["log_size"], df_fit["log_requests"], 1)
        df["residual"] = df["log_requests"] - np.polyval(coeffs, df["log_size"])
        slope, intercept = coeffs
        ann = f"log(R) = {slope:.2f}·log(S) + {intercept:.2f}"
    else:
        df["residual"] = 0.0
        ann = "(insufficient data for fit)"

    # Residual vs size, coloured by asset type
    c_r = df["asset_type"].map(type_colors).fillna("gray")
    ax_res0.scatter(df["log_size"], df["residual"], c=c_r, alpha=0.45, s=16, linewidths=0)
    ax_res0.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax_res0.set_xlabel("Asset size (bytes, log₁₀)")
    ax_res0.set_ylabel("Residual streaming requests\n(actual − size-predicted, log₁₀)")
    ax_res0.set_title(f"Residuals vs. size  [{ann}]", fontsize=8)
    ax_res0.grid(True, alpha=0.2)
    _xtick(ax_res0, [1e3, 1e6, 1e9, 1e12], ["1 KB", "1 MB", "1 GB", "1 TB"])
    for label, color in type_colors.items():
        if label in df["asset_type"].values:
            ax_res0.scatter([], [], c=color, label=label, s=20)
    ax_res0.legend(fontsize=7)

    # Residual vs avg_path_depth
    df_r2 = df[df["avg_path_depth"].notna()]
    if len(df_r2):
        c_r2 = df_r2["asset_type"].map(type_colors).fillna("gray")
        ax_res1.scatter(df_r2["avg_path_depth"], df_r2["residual"], c=c_r2, alpha=0.45, s=16, linewidths=0)
    ax_res1.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax_res1.set_xlabel("Avg internal path depth")
    ax_res1.set_ylabel("Residual streaming requests (log₁₀)")
    ax_res1.set_title("Residuals vs. path depth\n(depth-driven popularity?)", fontsize=9)
    ax_res1.grid(True, alpha=0.2)

    # Residual vs n_objects
    if len(df_obj) and "residual" in df_obj.columns:
        df_obj2 = df.loc[df_obj.index]
        c_o2 = df_obj2["asset_type"].map(type_colors).fillna("gray")
        ax_res2.scatter(df_obj2["log_n_objects"], df_obj2["residual"], c=c_o2, alpha=0.45, s=16, linewidths=0)
    ax_res2.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax_res2.set_xlabel("Internal objects (log₁₀)")
    ax_res2.set_ylabel("Residual streaming requests (log₁₀)")
    ax_res2.set_title("Residuals vs. object count\n(complexity-driven popularity?)", fontsize=9)
    ax_res2.grid(True, alpha=0.2)

    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved plot to {out_path}")


def _xtick(ax, vals, labels):
    ax.set_xticks([np.log10(v) for v in vals])
    ax.set_xticklabels(labels, fontsize=7)


def _ytick(ax, vals):
    ax.set_yticks([np.log10(v) for v in vals])
    ax.set_yticklabels([str(v) for v in vals], fontsize=7)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--metadata", required=True, type=pathlib.Path, help="CSV produced by prefetch_asset_metadata.py"
    )
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path, help="Root extraction cache directory")
    parser.add_argument(
        "--dataset", default=None, help="Optional dandiset ID substring to filter (e.g. 'DANDI:000123')"
    )
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument("--out", default="complexity_assessment.png", type=pathlib.Path)
    args = parser.parse_args()

    meta = pd.read_csv(args.metadata)
    print(f"Loaded metadata for {len(meta):,} assets ({meta['asset_type'].value_counts().to_dict()})")

    counts = load_streaming_counts(
        cache_dir=args.cache_dir,
        dataset_filter=args.dataset,
        use_encryption=not args.no_encryption,
    )
    print(f"Loaded streaming counts for {len(counts):,} assets")

    # Join on dandiset_id + asset_path
    merged = pd.merge(meta, counts, on=["dandiset_id", "asset_path"], how="inner")
    print(f"Joined: {len(merged):,} assets with both metadata and streaming data")

    if len(merged) == 0:
        print("ERROR: no rows after join — check that dandiset IDs and asset paths match between the two sources.")
        return

    plot_complexity_vs_streaming(merged, out_path=args.out)

    # Print top contradictions
    merged2 = merged.copy()
    merged2["log_size"] = np.log10(merged2["size_bytes"].clip(lower=1))
    merged2["log_requests"] = np.log10(merged2["n_streaming_requests"].clip(lower=1))
    if len(merged2) > 5:
        coeffs = np.polyfit(merged2["log_size"], merged2["log_requests"], 1)
        merged2["residual"] = merged2["log_requests"] - np.polyval(coeffs, merged2["log_size"])
        print("\n--- Top 10 over-streamed assets (small but heavily accessed) ---")
        print(
            merged2.nlargest(10, "residual")[
                ["dandiset_id", "asset_path", "size_bytes", "n_streaming_requests", "residual"]
            ].to_string(index=False)
        )
        print("\n--- Top 10 under-streamed assets (large but rarely accessed) ---")
        print(
            merged2.nsmallest(10, "residual")[
                ["dandiset_id", "asset_path", "size_bytes", "n_streaming_requests", "residual"]
            ].to_string(index=False)
        )


if __name__ == "__main__":
    main()
