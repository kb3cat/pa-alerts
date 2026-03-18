#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

PAGE_URL = "https://www.firstenergycorp.com/content/customer/outages_help/current_outages_maps/my-town-search.html?selectedTab=2"

DEFAULT_OUTPUT = "data/power_outages_pa.json"
DEFAULT_PREVIOUS = "data/previous_power_outages_pa.json"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    m = re.search(r"-?\d+", text)
    return int(m.group(0)) if m else None


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).replace("%", "").strip()
    try:
        return float(text)
    except:
        return None


def clean_text(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


def make_key(county: str, municipality: str) -> str:
    return f"{county.lower()}|{municipality.lower()}"


def scrape(show_browser=False):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not show_browser, args=["--no-sandbox"])
        page = browser.new_page()

        page.goto(PAGE_URL, timeout=60000)
        page.wait_for_timeout(5000)

        rows = page.evaluate("""
        () => {
            function txt(el) {
                return (el?.innerText || "").replace(/\\s+/g, " ").trim();
            }

            let data = [];
            document.querySelectorAll("table").forEach(table => {
                table.querySelectorAll("tr").forEach(tr => {
                    const cells = Array.from(tr.querySelectorAll("td")).map(txt);
                    if (cells.length >= 3) {
                        data.push({
                            municipality: cells[0],
                            county: cells[1],
                            customers_out: cells[2],
                            customers_served: cells[3],
                            percent_out: cells[4],
                            etr: cells[5]
                        });
                    }
                });
            });

            return data;
        }
        """)

        browser.close()

    return rows


def load_previous(path: Path):
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    return {
        make_key(i["county"], i["municipality"]): i
        for i in data.get("items", [])
    }


def apply_changes(current, previous):
    summary = {"new": 0, "increasing": 0, "decreasing": 0, "unchanged": 0, "restored": 0}
    results = []
    seen = set()

    for item in current:
        key = make_key(item["county"], item["municipality"])
        seen.add(key)

        prev = previous.get(key, {})
        prev_out = safe_int(prev.get("customers_out")) or 0
        curr_out = safe_int(item.get("customers_out")) or 0

        change = curr_out - prev_out

        if prev_out == 0 and curr_out > 0:
            status = "new"
        elif curr_out == 0 and prev_out > 0:
            status = "restored"
        elif curr_out > prev_out:
            status = "increasing"
        elif curr_out < prev_out:
            status = "decreasing"
        else:
            status = "unchanged"

        summary[status] += 1

        item["customers_out"] = curr_out
        item["previous_customers_out"] = prev_out
        item["change"] = change
        item["status"] = status

        results.append(item)

    for key, prev in previous.items():
        if key not in seen and safe_int(prev.get("customers_out")):
            results.append({
                "municipality": prev["municipality"],
                "county": prev["county"],
                "customers_out": 0,
                "previous_customers_out": prev["customers_out"],
                "change": -prev["customers_out"],
                "status": "restored"
            })
            summary["restored"] += 1

    return results, summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--previous", default=DEFAULT_PREVIOUS)
    args = parser.parse_args()

    output = Path(args.output)
    previous = Path(args.previous)

    output.parent.mkdir(parents=True, exist_ok=True)

    if output.exists():
        shutil.copyfile(output, previous)

    prev_data = load_previous(previous)
    current = scrape()

    items, summary = apply_changes(current, prev_data)

    result = {
        "fetched_at": iso_utc_now(),
        "count": len(items),
        "summary": summary,
        "items": items
    }

    output.write_text(json.dumps(result, indent=2))

    print("Done:", summary)


if __name__ == "__main__":
    main()
