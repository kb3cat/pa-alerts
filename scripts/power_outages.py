#!/usr/bin/env python3
"""
power_outages.py
----------------
Scrape FirstEnergy Pennsylvania outage data from the rendered "My Town" page
using Playwright DOM extraction instead of the older .areaSearch.json endpoint.

Why this version:
- Avoids the 400/HTML response from my-town-search.areaSearch.json
- Works from the rendered page itself
- Extracts Pennsylvania rows from the visible PA search table
- Writes normalized JSON for your dashboard/workflow

Requirements:
    pip install playwright
    playwright install chromium

Usage:
    python3 scripts/power_outages.py
    python3 scripts/power_outages.py --output data/power_outages.json
    python3 scripts/power_outages.py --show-browser
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

PAGE_URL = (
    "https://www.firstenergycorp.com/content/customer/outages_help/"
    "current_outages_maps/my-town-search.html?selectedTab=2"
)

DEFAULT_OUTPUT = "data/power_outages.json"


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


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    municipality = clean_text(raw.get("municipality"))
    county = clean_text(raw.get("county"))
    company = clean_text(raw.get("company"))
    state = clean_text(raw.get("state") or "PA")
    customers_out = safe_int(raw.get("customers_out"))
    customers_served = safe_int(raw.get("customers_served"))
    percent_out = raw.get("percent_out")
    etr = clean_text(raw.get("etr")) or None
    last_updated = clean_text(raw.get("last_updated")) or None
    details = clean_text(raw.get("details")) or None

    if percent_out is not None:
        try:
            percent_out = float(str(percent_out).replace("%", "").strip())
        except ValueError:
            percent_out = None

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

            # Try to dismiss the "Oops / taking longer than expected" overlay if present.
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

            # Force NY & PA tab if needed.
            tab_candidates = [
                "text='NY & PA'",
                "a:has-text('NY & PA')",
                "button:has-text('NY & PA')",
            ]
            for selector in tab_candidates:
                try:
                    locator = page.locator(selector).first
                    if locator.is_visible(timeout=1500):
                        locator.click(timeout=1500)
                        page.wait_for_timeout(2500)
                        break
                except Exception:
                    pass

            # Wait for the Pennsylvania Search heading to exist.
            page.wait_for_selector("text=Pennsylvania Search", timeout=timeout_ms)

            rows = page.evaluate(
                """
                () => {
                  function txt(el) {
                    return (el && el.innerText ? el.innerText : "").replace(/\\s+/g, " ").trim();
                  }

                  function pickSectionRoot() {
                    const headings = Array.from(document.querySelectorAll("h1,h2,h3,h4"));
                    const paHeading = headings.find(h => /Pennsylvania Search/i.test(txt(h)));
                    if (!paHeading) return null;

                    let node = paHeading.parentElement;
                    while (node) {
                      const tables = node.querySelectorAll("table");
                      if (tables.length) return node;
                      node = node.parentElement;
                    }
                    return paHeading.parentElement || document.body;
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
                      const keys = Object.keys(row);
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

                        // Flexible positional fallback
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

                  const root = pickSectionRoot();
                  const tables = root ? Array.from(root.querySelectorAll("table")) : [];
                  let rows = [];

                  for (const table of tables) {
                    rows = rows.concat(tableToObjects(table));
                  }

                  // Fallback: if PA section tables were not found, scan all tables on page.
                  if (!rows.length) {
                    const allTables = Array.from(document.querySelectorAll("table"));
                    for (const table of allTables) {
                      rows = rows.concat(tableToObjects(table));
                    }
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

    cleaned = []
    for row in rows:
        if not isinstance(row, dict):
            continue

        item = normalize_item(row)

        # Keep only rows that look like actual PA municipality records.
        if not any([
            item["municipality"],
            item["county"],
            item["customers_out"] is not None,
            item["customers_served"] is not None,
            item["percent_out"] is not None,
        ]):
            continue

        cleaned.append(item)

    # Deduplicate loosely.
    deduped = []
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


def build_output(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "name": "firstenergy_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source_page": PAGE_URL,
        "count": len(items),
        "items": items,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Scrape FirstEnergy Pennsylvania outage data from rendered page via Playwright."
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--show-browser",
        action="store_true",
        help="Show Chromium while running.",
    )
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        items = scrape_pa_rows(show_browser=args.show_browser)
        output = build_output(items)
        output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(f"Wrote {output['count']} Pennsylvania outage rows to {output_path}")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
