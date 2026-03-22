#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests


INDEX_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
OUTPUT_PATH = Path("data/raw/StormEvents_details_latest.csv.gz")
META_PATH = Path("data/raw/StormEvents_details_latest.meta.json")
USER_AGENT = "PennAlerts/1.0 (GitHub Actions)"

# Example:
# StormEvents_details-ftp_v1.0_d2025_c20260318.csv.gz
DETAILS_RE = re.compile(
    r'(StormEvents_details-ftp_v1\.0_d(?P<data_year>\d{4})_c(?P<create_date>\d{8})\.csv\.gz)'
)


def get_index_html() -> str:
    r = requests.get(
        INDEX_URL,
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,*/*"},
        timeout=60,
    )
    r.raise_for_status()
    return r.text


def find_latest_details_file(html: str) -> tuple[str, str, str]:
    matches = []
    for m in DETAILS_RE.finditer(html):
        filename = m.group(1)
        data_year = m.group("data_year")
        create_date = m.group("create_date")
        matches.append((filename, data_year, create_date))

    if not matches:
        raise RuntimeError("Could not find any StormEvents details CSV files in the NCEI index.")

    # Sort by data year first, then creation date
    matches.sort(key=lambda x: (int(x[1]), x[2]), reverse=True)
    return matches[0]


def download_file(filename: str, output_path: Path) -> None:
    file_url = urljoin(INDEX_URL, filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(
        file_url,
        headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
        timeout=120,
        stream=True,
    ) as r:
        r.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def write_meta(filename: str, data_year: str, create_date: str, meta_path: Path) -> None:
    import json

    meta = {
        "source_index": INDEX_URL,
        "filename": filename,
        "data_year": data_year,
        "create_date": create_date,
        "download_url": urljoin(INDEX_URL, filename),
    }
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def main() -> int:
    html = get_index_html()
    filename, data_year, create_date = find_latest_details_file(html)

    print(f"Latest NCEI details file: {filename}")
    download_file(filename, OUTPUT_PATH)
    write_meta(filename, data_year, create_date, META_PATH)
    print(f"Saved to: {OUTPUT_PATH}")
    print(f"Metadata saved to: {META_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
