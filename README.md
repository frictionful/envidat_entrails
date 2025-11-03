# EnviDat Entrails for Exploring & Visualizing the data types on Envidat

This script does three main things:

1. **Downloads metadata listings** from several S3-compatible endpoints.  
2. **Aggregates file details** (like name, extension, and size) into a unified CSV (`all_s3_files.csv`).  
3. **Visualizes the results** with interactive charts using Plotly.

You’ll get:
- Sankey diagrams for file-type distributions (by count *and* by size)
- Sunburst charts showing bucket and extension hierarchies (also by count *and* by size)
- A CSV summary for further analysis

## Buckets Analyzed

These are the five S3 endpoints we’re working with:

| Bucket | Description |
|--------|--------------|
| `envidat-doi` | Published EnviDat datasets with DOIs |
| `envicloud` | Internal EnviDat staging and mirrored datasets |
| `edna` | Elevation-derived hydrological data |
| `pointclouds` | drone-derived pointclouds |
| `drone-data` | drone-derived image data |

These are two buckets that were excluded becuase of their size (see filtering below for details).

| Bucket | Description |
|--------|--------------|
| `chelsav1` | CHELSA climate dataset (v1) |
| `chelsav2` | CHELSA climate dataset (v2) |

Each bucket is public on the SWITCH Cloud (`https://os.zhdk.cloud.switch.ch/<bucket-name>/`).

## Filtering
To keep the visualizations meaningful, two filtering rules were added to the **fetch** step (this is the default, production-ready behavior):

1. **Exclude any path that contains `envidat.1` (case-insensitive).**
   Rationale: `envidat.1` denotes certain DOI datasets that contain large numbers of `.raw` and other files and skew the distribution. Example excluded key:
   `10.16904_envidat.1/180f906a-5fc8-4a7e-b9f7-ab00f7092d79_04-GITS.html`

2. **In the `envidat-doi` bucket only, exclude `.html`, `.json`, and `.xml` files.**
   Rationale: These are machine-oriented metadata files (created for machine-to-machine interoperability) and we prefer to exclude them from the dataset-level content analysis so they don’t dominate counts.

These rules are applied **while fetching** the S3 listings (i.e., excluded rows never get written to the CSV). This keeps the CSV smaller and the analysis honest.

## Script Overview

**Typical run:**

```bash
#pull s3 file data
python3 entrails.py fetch --out all_s3_files.csv      

#visualize the data
python3 entrails.py visualize --csv all_s3_files.csv --out-prefix envidat_viz

#or do both
python envidat_filetype_tool.py run-all --out all_s3_files.csv --out-prefix envidat_viz
```

## What you’ll see in the logs

After running the fetch step you should see informative logs indicating how many items were skipped. Example:

```
2025-10-23 10:05:12,123 INFO: Got 1000 Contents entries (page 1)
...
2025-10-23 10:12:43,456 INFO: Skipped 2345 objects containing 'envidat.1' in their path for bucket envidat-doi
2025-10-23 10:12:43,456 INFO: Skipped 412 metadata files (.html/.json/.xml) in envidat-doi bucket envidat-doi
2025-10-23 10:12:43,457 INFO: Finished bucket: https://os.zhdk.cloud.switch.ch/envidat-doi/ (pages fetched=27)
```

**Outputs:**

* `all_s3_files.csv` - or whatever you call it in your CLI, a csv that holds all the file data scraped from the online buckets
* `out_sankey.html` — File type breakdown by **count**
* `out_sankey_size.html` — File type breakdown by **total bytes**
* `out_sunburst.html` — Hierarchical breakdown (bucket → extension) by **count**
* `out_sunburst_size.html` — Hierarchical breakdown (bucket → extension) by **total bytes**

## Visual Examples

| Visualization        | Description                                                   |
| -------------------- | ------------------------------------------------------------- |
| **Sankey (count)**   | Shows how many files of each type exist across all buckets.   |
| **Sankey (bytes)**   | Shows which file types are hogging the most disk space.       |
| **Sunburst (count)** | Visualizes the bucket→file-type hierarchy by number of files. |
| **Sunburst (bytes)** | Same hierarchy, but sized by total data volume (bytes).       |

## Requirements

* Python 3.8+
* `pandas`
* `plotly`
* `boto3`
* `tqdm`
* `requests`
* (Optional) `humanfriendly` for nicer byte-size logs

## Byte-Size Reality Checks

Ever see numbers like `34782943827` and think “uh, what”?
Use the built-in `human_bytes()` helper to print totals in GiB or TiB:

```python
from visualize_s3_data import human_bytes
print(human_bytes(34782943827))  # 32.4GiB
```

Because nobody wants to count bytes manually.

## Troubleshooting

**Q:** “I got an Access Denied error.”
**A:** Some buckets or objects might be private. The script skips those gracefully.

**Q:** “My sunburst looks weird.”
**A:** Make sure the CSV includes valid `bucket_name` and `extension` columns. Use `process_s3_data.py` to regenerate a clean one.

**Q:** “The Sankey is all gray.”
**A:** That’s Plotly’s default. Try toggling to ‘Dark Mode’ in your browser and pretend it’s a design choice.

## License

MIT License — use, remix, or extend freely.