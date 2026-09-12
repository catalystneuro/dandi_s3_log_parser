# Access vs. structure analysis

Exploratory analysis relating the **internal structure** and **size** of valid
NWB files on the DANDI Archive to how much they are **accessed** on the web.

It answers: *do structural-complexity metrics (group count, dataset count, total
cophenetic index, out-degree stats) predict web access, and how does asset size
compare?*

## Data sources (all public, fetched over HTTPS)

| Quantity | Source | Key |
|---|---|---|
| groups / datasets | `dandi-cache/valid-nwb-file-to-number-of-*` caches (`dist` branch, gzipped) | content ID |
| cophenetic index / out-degrees | `dandi-cache/valid-nwb-file-to-{cophenetic-index,out-degrees}` (`derivatives` branch, plain jsonl) | content ID |
| content ID → (dandiset, asset path) | `dandi-cache/content-id-to-nwb-file` cache (`dist` branch) | content ID |
| requests / downloads | `dandi/access-summaries` `content/summaries/<dandiset>/by_asset.tsv` | (dandiset, asset path) |
| asset size (bytes) | S3 `HEAD` on `dandiarchive.s3.amazonaws.com/blobs/<c[:3]>/<c[3:6]>/<content_id>` | content ID |

The DANDI REST API is **not** used (it is frequently firewalled); asset sizes are
read straight from S3 object headers instead.

## Usage

```bash
pip install requests tqdm numpy pandas matplotlib

# 1. Build the joined table (network-heavy: fetches caches, per-dandiset TSVs,
#    and one S3 HEAD per file). Produces access_structure.csv.
python build_dataset.py --out access_structure.csv --workers 20

# 2. Render figures + print the correlation summary.
python plot_relationships.py --data access_structure.csv --out-dir figures/
```

## Output columns (`access_structure.csv`)

`content_id, dandiset_id, asset_path, groups, datasets, cophenetic_index,
mean_out_degree, max_out_degree, variance_out_degree, n_internal_nodes,
size_bytes, number_of_requests, number_of_downloads, requests_censored`

`number_of_requests` / `number_of_downloads` are privacy-rounded at the source;
values reported as `"<N"` are decoded to `N/2` and marked in `requests_censored`.
Streaming is derived as `number_of_requests - number_of_downloads`.

## Findings (snapshot, ~4,493 files across ~253 dandisets)

- **The structural metrics are one axis.** Groups, datasets, and the total
  cophenetic index are rank-correlated 0.82–0.95; the cophenetic index is a
  nonlinear restatement of object count (see `SUPPLEMENT_tree_metrics.md` for why
  it replaces the binary-baseline Sackin index used by the source cache).
- **Structural complexity barely predicts access volume** (Spearman ≈ 0.10–0.19
  vs. total or streaming requests; log-log ≈ 0.37–0.42). It is a poor
  access-normalizer.
- **Complexity predicts consumption *mode*, not volume.** The download *fraction*
  falls with complexity (Spearman ≈ −0.37): complex files are read via partial
  range requests ("scrubbed"), simple files are downloaded whole.
- **Asset size is the dominant predictor of streaming** (Spearman ≈ +0.75,
  log-log ≈ +0.70) — far stronger than any structural metric.
- **Size and structural complexity are nearly orthogonal** (Spearman ≈ −0.11),
  and complexity still adds an independent signal after controlling for size
  (partial r ≈ +0.49): at equal size, more complex files are streamed more.

Numbers will drift as the caches and access summaries update; re-run to refresh.
