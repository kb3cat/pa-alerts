#!/usr/bin/env python3
"""
power_outages.py
----------------
Scrape FirstEnergy Pennsylvania outage data from the rendered "My Town" page
using Playwright DOM extraction, then add change tracking against the prior run.

Outputs:
- current file (default): data/power_outages_pa.json
- previous snapshot file:  data/previous_power_outages_pa.json

Change tracking fields per item:
- previous_customers_out
- change
- status: new | increasing | decreasing | unchanged | restored

Requirements:
    pip install playwright
    playwright install chromium

Usage:
    python scripts/power_outages.py
    python scripts/power_outages.py --output data/power_outages_pa.json
    python scripts/power_outages.py --previous data/previous_power_outages_pa.json
    python scripts/power_outages.py --show-browser
"""

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

PAGE_URL = (
    "https://www.firstenergycorp.com/content/customer/outages_help/"
    "current_outages_maps/my-town-search.html?selectedTab=2"
)

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
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace("%", "").replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    return re.sub(r"\s+", " ", text).strip()


def make_key(county: str, municipality: str) -> str:
    return f"{clean_text(county).strip().lower()}|{clean_text(municipality).strip().lower()}"


def normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    municipality = clean_text(raw.get("municipality"))
    county = clean_text(raw.get("county"))
    company = clean_text(raw.get("company"))
    state = clean_text(raw.get("state") or "PA")
    customers_out = safe_int(raw.get("customers_out"))
    customers_served = safe_int(raw.get("customers_served"))
    percent_out = safe_float(raw.get("percent_out"))
    etr = clean_text(raw.get("etr")) or None
    last_updated = clean_text(raw.get("last_updated")) or None
    details = clean_text(raw.get("details")) or None

    return {
        "municipality": municipality,
        "county": county,
        "state": state,
        "company": company,
        "customers_out": customers_out,
        "customers_served": customers_served,
        "percent_out": percent_out,
        "etr": etr,
        "last_updated": last_updated,
        "details": details,
        "raw": raw,
    }


def scrape_pa_rows(show_browser: bool = False, timeout_ms: int = 60000) -> List[Dict[str, Any]]:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not show_browser,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        page = browser.new_page()

        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(5000)

            # Try to close modal/overlay if present
            for selector in [
                "text=CLOSE",
                "button:has-text('CLOSE')",
                "[aria-label='Close']",
                ".modal button",
            ]:
                try:
                    locator = page.locator(selector).first
                    if locator.is_visible(timeout=1000):
                        locator.click(timeout=1000)
                        page.wait_for_timeout(750)
                        break
                except Exception:
                    pass

            # Try to click NY & PA tab if present
            for selector in [
                "text='NY & PA'",
                "a:has-text('NY & PA')",
                "button:has-text('NY & PA')",
            ]:
                try:
                    locator = page.locator(selector).first
                    if locator.is_visible(timeout=1500):
                        locator.click(timeout=1500)
                        page.wait_for_timeout(2500)
                        break
                except Exception:
                    pass

            page.wait_for_timeout(3000)

            rows = page.evaluate(
                """
                () => {
                  function txt(el) {
                    return (el && el.innerText ? el.innerText : "").replace(/\\s+/g, " ").trim();
                  }

                  function tableToObjects(table) {
                    const trs = Array.from(table.querySelectorAll("tr"));
                    if (!trs.length) return [];

                    let headers = [];
                    const firstCells = Array.from(trs[0].querySelectorAll("th,td")).map(txt);
                    const maybeHeader = firstCells.some(v =>
                      /county|municipality|town|city|customers|out|served|percent|etr|updated/i.test(v)
                    );

                    let startIndex = 0;
                    if (maybeHeader) {
                      headers = firstCells.map(h => h.toLowerCase());
                      startIndex = 1;
                    }

                    const out = [];
                    for (let i = startIndex; i < trs.length; i++) {
                      const cells = Array.from(trs[i].querySelectorAll("td,th")).map(txt).filter(Boolean);
                      if (!cells.length) continue;
                      if (cells.length === 1 && /no outages/i.test(cells[0])) continue;

                      let row = {};
                      if (headers.length && headers.length === cells.length) {
                        headers.forEach((h, idx) => { row[h] = cells[idx]; });
                      } else {
                        row = { cells };
                      }
                      out.push(row);
                    }
                    return out;
                  }

                  function normalizeRows(rawRows) {
                    const normalized = [];

                    for (const row of rawRows) {
                      let municipality = "";
                      let county = "";
                      let company = "";
                      let customers_out = null;
                      let customers_served = null;
                      let percent_out = null;
                      let etr = "";
                      let last_updated = "";
                      let details = "";

                      if (row.cells) {
                        const cells = row.cells;

                        if (cells.length >= 1) municipality = cells[0];
                        if (cells.length >= 2) county = cells[1];
                        if (cells.length >= 3) customers_out = cells[2];
                        if (cells.length >= 4) customers_served = cells[3];
                        if (cells.length >= 5) percent_out = cells[4];
                        if (cells.length >= 6) etr = cells[5];
                        if (cells.length >= 7) details = cells.slice(6).join(" | ");
                      } else {
                        for (const [k, v] of Object.entries(row)) {
                          const key = k.toLowerCase();
                          if (/municipality|town|city|area|name/.test(key) && !municipality) municipality = v;
                          else if (/county/.test(key) && !county) county = v;
                          else if (/company|utility|operating/.test(key) && !company) company = v;
                          else if (/customers.*out|cust.*out|affected|outages|out\\b/.test(key) && customers_out === null) customers_out = v;
                          else if (/customers.*served|cust.*served|served|total customers/.test(key) && customers_served === null) customers_served = v;
                          else if (/percent|pct/.test(key) && percent_out === null) percent_out = v;
                          else if (/etr|estimated restoration|restoration/.test(key) && !etr) etr = v;
                          else if (/updated|last updated|timestamp|as of/.test(key) && !last_updated) last_updated = v;
                          else details += (details ? " | " : "") + `${k}: ${v}`;
                        }
                      }

                      const useful =
                        municipality || county || customers_out !== null || customers_served !== null || percent_out !== null;

                      if (!useful) continue;

                      normalized.push({
                        municipality,
                        county,
                        company,
                        state: "PA",
                        customers_out,
                        customers_served,
                        percent_out,
                        etr,
                        last_updated,
                        details
                      });
                    }

                    return normalized;
                  }

                  const allTables = Array.from(document.querySelectorAll("table"));
                  let rows = [];
                  for (const table of allTables) {
                    rows = rows.concat(tableToObjects(table));
                  }

                  return normalizeRows(rows);
                }
                """
            )

        except PlaywrightTimeoutError as exc:
            browser.close()
            raise RuntimeError(f"Timed out loading FirstEnergy page: {exc}") from exc
        except Exception:
            browser.close()
            raise

        browser.close()

    if not isinstance(rows, list):
        raise RuntimeError("Unexpected scrape result: rows is not a list.")

    cleaned: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        item = normalize_item(row)

        if not any([
            item["municipality"],
            item["county"],
            item["customers_out"] is not None,
            item["customers_served"] is not None,
            item["percent_out"] is not None,
        ]):
            continue

        cleaned.append(item)

    # Deduplicate
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for item in cleaned:
        key = (
            item["municipality"].lower(),
            item["county"].lower(),
            item["customers_out"],
            item["customers_served"],
            item["percent_out"],
            item["etr"],
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    deduped.sort(
        key=lambda x: (
            -(x["customers_out"] if isinstance(x["customers_out"], int) else -1),
            x["county"].lower(),
            x["municipality"].lower(),
        )
    )

    return deduped


def load_previous_items(previous_path: Path) -> Dict[str, Dict[str, Any]]:
    if not previous_path.exists():
        return {}

    try:
        payload = json.loads(previous_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    items = payload.get("items", [])
    if not isinstance(items, list):
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        key = make_key(item.get("county", ""), item.get("municipality", ""))
        if key == "|":
            continue
        out[key] = item
    return out


def apply_change_tracking(current_items: List[Dict[str, Any]], previous_lookup: Dict[str, Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    tracked_items: List[Dict[str, Any]] = []

    summary = {
        "new": 0,
        "increasing": 0,
        "decreasing": 0,
        "unchanged": 0,
        "restored": 0,
    }

    current_keys = set()

    for item in current_items:
        key = make_key(item.get("county", ""), item.get("municipality", ""))
        current_keys.add(key)

        previous = previous_lookup.get(key, {})
        prev_out = safe_int(previous.get("customers_out"))
        curr_out = safe_int(item.get("customers_out"))

        if prev_out is None:
            prev_out = 0
        if curr_out is None:
            curr_out = 0

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

        enriched = dict(item)
        enriched["previous_customers_out"] = prev_out
        enriched["change"] = change
        enriched["status"] = status
        tracked_items.append(enriched)

    # Add restored rows that disappeared entirely from current scrape.
    # This is useful if FE removes zero-outage rows from the table.
    for key, previous in previous_lookup.items():
        if key in current_keys:
            continue

        prev_out = safe_int(previous.get("customers_out"))
        if prev_out is None or prev_out <= 0:
            continue

        restored = {
            "municipality": clean_text(previous.get("municipality")),
            "county": clean_text(previous.get("county")),
            "state": clean_text(previous.get("state") or "PA"),
            "company": clean_text(previous.get("company")),
            "customers_out": 0,
            "customers_served": safe_int(previous.get("customers_served")),
            "percent_out": 0.0,
            "etr": None,
            "last_updated": iso_utc_now(),
            "details": "Not present in current scrape; treated as restored.",
            "raw": {"synthetic_restored_row": True},
            "previous_customers_out": prev_out,
            "change": -prev_out,
            "status": "restored",
        }
        tracked_items.append(restored)
        summary["restored"] += 1

    tracked_items.sort(
        key=lambda x: (
            {"new": 0, "increasing": 1, "decreasing": 2, "restored": 3, "unchanged": 4}.get(x.get("status", ""), 9),
            -(x["customers_out"] if isinstance(x["customers_out"], int) else -1),
            x.get("county", "").lower(),
            x.get("municipality", "").lower(),
        )
    )

    return tracked_items, summary


def build_county_summary(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counties: Dict[str, Dict[str, Any]] = {}

    for item in items:
        county = clean_text(item.get("county")) or "Unknown"
        entry = counties.setdefault(
            county,
            {
                "county": county,
                "municipalities": 0,
                "customers_out": 0,
                "new": 0,
                "increasing": 0,
                "decreasing": 0,
                "restored": 0,
                "unchanged": 0,
            },
        )

        entry["municipalities"] += 1
        entry["customers_out"] += safe_int(item.get("customers_out")) or 0

        status = item.get("status")
        if status in ("new", "increasing", "decreasing", "restored", "unchanged"):
            entry[status] += 1

    out = list(counties.values())
    out.sort(key=lambda x: (-x["customers_out"], x["county"].lower()))
    return out


def build_output(items: List[Dict[str, Any]], summary: Dict[str, int]) -> Dict[str, Any]:
    active_count = sum(1 for i in items if (safe_int(i.get("customers_out")) or 0) > 0)
    total_customers_out = sum((safe_int(i.get("customers_out")) or 0) for i in items)

    return {
        "name": "firstenergy_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source_page": PAGE_URL,
        "count": len(items),
        "active_count": active_count,
        "total_customers_out": total_customers_out,
        "summary": summary,
        "county_summary": build_county_summary(items),
        "items": items,
    }


def backup_current_to_previous(output_path: Path, previous_path: Path) -> None:
    if output_path.exists():
        previous_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(output_path, previous_path)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape FirstEnergy Pennsylvania outage data and add change tracking."
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--previous",
        default=DEFAULT_PREVIOUS,
        help=f"Previous snapshot JSON path (default: {DEFAULT_PREVIOUS})",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Show Chromium while running.",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    previous_path = Path(args.previous)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    previous_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Preserve prior current file before writing a new one
        backup_current_to_previous(output_path, previous_path)

        previous_lookup = load_previous_items(previous_path)
        current_items = scrape_pa_rows(show_browser=args.show_browser)
        tracked_items, summary = apply_change_tracking(current_items, previous_lookup)
        output = build_output(tracked_items, summary)

        output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

        print(f"Wrote {output['count']} Pennsylvania outage rows to {output_path}")
        print(f"Active rows: {output['active_count']}")
        print(f"Total customers out: {output['total_customers_out']}")
        print(
            "Changes: "
            f"new={summary['new']} "
            f"increasing={summary['increasing']} "
            f"decreasing={summary['decreasing']} "
            f"restored={summary['restored']} "
            f"unchanged={summary['unchanged']}"
        )
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
