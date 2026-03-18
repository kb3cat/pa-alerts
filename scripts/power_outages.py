#!/usr/bin/env python3
"""
power_outages.py
----------------
Scrapes FirstEnergy Pennsylvania "My Town" outage data using Playwright page-context
fetch() so the request is made from inside the browser session/origin.

Why this version:
- Avoids curl/requests issues with direct endpoint access
- Uses page.evaluate(...) -> fetch(...) from the FirstEnergy page context
- Filters Pennsylvania only
- Writes a normalized JSON file

Requirements:
    pip install playwright
    playwright install chromium

Usage:
    python3 power_outages.py

Optional:
    python3 power_outages.py --output data/power_outages.json
    python3 power_outages.py --show-browser
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# FirstEnergy "My Town" page for NY & PA
PAGE_URL = (
    "https://www.firstenergycorp.com/content/customer/outages_help/"
    "current_outages_maps/my-town-search.html?selectedTab=2"
)

# This is the endpoint that has been used for municipality/area search JSON.
# We call it from the browser page context instead of directly from Python.
AREA_SEARCH_URL = (
    "https://www.firstenergycorp.com/content/customer/outages_help/"
    "current_outages_maps/my-town-search.areaSearch.json"
)

DEFAULT_OUTPUT = "data/power_outages.json"


def iso_utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    text = str(value).strip().replace(",", "")
    if text == "":
        return None
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def first_nonempty(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for key in keys:
        if key in d:
            value = d[key]
            if value is not None and str(value).strip() != "":
                return value
    return default


def looks_like_pa(record: Dict[str, Any]) -> bool:
    # Be generous; FE field names can shift.
    state = str(first_nonempty(record, ["state", "stateCode", "st", "serviceState"], "")).strip().upper()
    company = str(first_nonempty(record, ["company", "operatingCompany", "utility", "opco"], "")).strip().upper()
    county = str(first_nonempty(record, ["county", "countyName"], "")).strip()
    muni = str(first_nonempty(record, ["municipality", "municipalityName", "areaName", "city", "town"], "")).strip()

    if state == "PA" or state == "PENNSYLVANIA":
        return True

    # Common FirstEnergy PA operating companies
    pa_companies = {
        "PENELEC",
        "MET-ED",
        "MET ED",
        "PENN POWER",
        "WEST PENN POWER",
    }
    if company in pa_companies:
        return True

    # Sometimes records include enough fields to infer they're valid area rows;
    # keep them only if not explicitly another state.
    other_states = {"OH", "NJ", "NY", "MD", "WV"}
    if state and state in other_states:
        return False

    # If it has a PA-looking company or no state at all, keep it for now.
    if county or muni:
        return True

    return False


def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    municipality = first_nonempty(
        raw,
        [
            "municipality",
            "municipalityName",
            "areaName",
            "city",
            "town",
            "name",
            "area",
        ],
        "",
    )

    county = first_nonempty(raw, ["county", "countyName"], "")
    state = first_nonempty(raw, ["state", "stateCode", "st", "serviceState"], "PA")
    company = first_nonempty(raw, ["company", "operatingCompany", "utility", "opco"], "")

    customers_out = safe_int(
        first_nonempty(
            raw,
            [
                "customersOut",
                "custOut",
                "out",
                "outages",
                "affectedCustomers",
                "customerCount",
            ],
        )
    )

    customers_served = safe_int(
        first_nonempty(
            raw,
            [
                "customersServed",
                "custServed",
                "served",
                "totalCustomers",
            ],
        )
    )

    percent_out = first_nonempty(raw, ["percentOut", "pctOut", "percent"], None)
    if percent_out is not None:
        try:
            percent_out = float(str(percent_out).replace("%", "").strip())
        except ValueError:
            percent_out = None

    last_updated = first_nonempty(
        raw,
        ["lastUpdated", "last_updated", "updateTime", "timestamp", "asOf"],
        None,
    )

    etr = first_nonempty(
        raw,
        [
            "etr",
            "ETR",
            "estimatedRestoration",
            "estimatedRestorationTime",
            "restorationTime",
        ],
        None,
    )

    cause = first_nonempty(raw, ["cause", "outageCause"], None)

    normalized = {
        "municipality": municipality,
        "county": county,
        "state": state,
        "company": company,
        "customers_out": customers_out,
        "customers_served": customers_served,
        "percent_out": percent_out,
        "etr": etr,
        "cause": cause,
        "last_updated": last_updated,
        "raw": raw,
    }

    return normalized


def extract_records(payload: Any) -> List[Dict[str, Any]]:
    """
    Handle common JSON shapes:
    - list[dict]
    - {"data":[...]}
    - {"items":[...]}
    - {"results":[...]}
    - {"areas":[...]}
    """
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        for key in ["data", "items", "results", "areas", "rows", "municipalities"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]

        # Fallback: if dict itself looks like a record collection container but nested oddly,
        # gather dicts from top-level lists.
        out: List[Dict[str, Any]] = []
        for value in payload.values():
            if isinstance(value, list):
                out.extend([x for x in value if isinstance(x, dict)])
        if out:
            return out

    return []


def fetch_area_json(show_browser: bool = False, timeout_ms: int = 45000) -> Any:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not show_browser)
        page = browser.new_page()

        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=timeout_ms)
            page.wait_for_timeout(3000)

            result = page.evaluate(
                """
                async ({ areaUrl }) => {
                  try {
                    const resp = await fetch(areaUrl, {
                      method: "GET",
                      credentials: "include",
                      headers: {
                        "Accept": "application/json, text/plain, */*",
                        "X-Requested-With": "XMLHttpRequest"
                      }
                    });

                    const text = await resp.text();
                    let data = null;
                    let parseError = null;

                    try {
                      data = JSON.parse(text);
                    } catch (err) {
                      parseError = String(err);
                    }

                    return {
                      ok: resp.ok,
                      status: resp.status,
                      statusText: resp.statusText,
                      url: resp.url,
                      text: text,
                      data: data,
                      parseError: parseError
                    };
                  } catch (err) {
                    return {
                      ok: false,
                      status: 0,
                      statusText: "FETCH_ERROR",
                      url: areaUrl,
                      text: "",
                      data: null,
                      parseError: String(err)
                    };
                  }
                }
                """,
                {"areaUrl": AREA_SEARCH_URL},
            )

        except PlaywrightTimeoutError as exc:
            browser.close()
            raise RuntimeError(f"Timed out loading FirstEnergy page: {exc}") from exc
        except Exception:
            browser.close()
            raise

        browser.close()

    if not result.get("ok"):
        raise RuntimeError(
            f"FirstEnergy fetch failed: status={result.get('status')} "
            f"statusText={result.get('statusText')} "
            f"parseError={result.get('parseError')}"
        )

    if result.get("data") is not None:
        return result["data"]

    # Sometimes server may respond with JSON-ish text but the parse fails
    text = result.get("text", "").strip()
    if not text:
        raise RuntimeError("FirstEnergy fetch returned empty response text.")

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        preview = text[:500].replace("\n", " ")
        raise RuntimeError(
            f"Response was not valid JSON. Parse error: {exc}. Preview: {preview}"
        ) from exc


def build_output(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    cleaned: List[Dict[str, Any]] = []

    for raw in records:
        if not looks_like_pa(raw):
            continue
        cleaned.append(normalize_record(raw))

    # Sort: largest outages first, then county, then municipality
    cleaned.sort(
        key=lambda x: (
            -(x["customers_out"] if isinstance(x["customers_out"], int) else -1),
            str(x["county"]).lower(),
            str(x["municipality"]).lower(),
        )
    )

    return {
        "name": "firstenergy_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source_page": PAGE_URL,
        "source_api": AREA_SEARCH_URL,
        "count": len(cleaned),
        "items": cleaned,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch FirstEnergy Pennsylvania municipality outage data via Playwright page-context fetch()."
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
        payload = fetch_area_json(show_browser=args.show_browser)
        records = extract_records(payload)

        if not records:
            # Write debug payload too, so you can inspect shape changes
            debug_path = output_path.with_suffix(".debug.json")
            debug_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            raise RuntimeError(
                f"No records found in response. Wrote raw payload to {debug_path}"
            )

        output = build_output(records)
        output_path.write_text(json.dumps(output, indent=2), encoding="utf-8")

        print(f"Wrote {output['count']} Pennsylvania outage rows to {output_path}")
        return 0

    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
