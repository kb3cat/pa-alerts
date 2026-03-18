#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

PAGE_URL = (
    "https://www.firstenergycorp.com/content/customer/outages_help/"
    "current_outages_maps/my-town-search.html?selectedTab=2"
)

AREA_SEARCH_ENDPOINT = "my-town-search.areaSearch.json"

DEFAULT_OUTPUT = "data/power_outages_pa.json"
DEFAULT_PREVIOUS = "data/previous_power_outages_pa.json"
DEFAULT_DEBUG_HTML = "data/power_outages_debug.html"
DEFAULT_DEBUG_JSON = "data/power_outages_debug.json"
DEFAULT_DEBUG_SCREENSHOT = "data/power_outages_debug.png"

PA_COUNTIES = {
    "ADAMS", "ALLEGHENY", "ARMSTRONG", "BEAVER", "BEDFORD", "BERKS", "BLAIR",
    "BRADFORD", "BUCKS", "BUTLER", "CAMBRIA", "CAMERON", "CARBON", "CENTRE",
    "CHESTER", "CLARION", "CLEARFIELD", "CLINTON", "COLUMBIA", "CRAWFORD",
    "CUMBERLAND", "DAUPHIN", "DELAWARE", "ELK", "ERIE", "FAYETTE", "FOREST",
    "FRANKLIN", "FULTON", "GREENE", "HUNTINGDON", "INDIANA", "JEFFERSON",
    "JUNIATA", "LACKAWANNA", "LANCASTER", "LAWRENCE", "LEBANON", "LEHIGH",
    "LUZERNE", "LYCOMING", "MCKEAN", "MERCER", "MIFFLIN", "MONROE",
    "MONTGOMERY", "MONTOUR", "NORTHAMPTON", "NORTHUMBERLAND", "PERRY",
    "PHILADELPHIA", "PIKE", "POTTER", "SCHUYLKILL", "SNYDER", "SOMERSET",
    "SULLIVAN", "SUSQUEHANNA", "TIOGA", "UNION", "VENANGO", "WARREN",
    "WASHINGTON", "WAYNE", "WESTMORELAND", "WYOMING", "YORK",
}


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


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


def make_key(county: str, municipality: str) -> str:
    return f"{clean_text(county).lower()}|{clean_text(municipality).lower()}"


def write_debug_files(
    page,
    debug_html_path: Path,
    debug_json_path: Path,
    debug_screenshot_path: Path,
    extra: Dict[str, Any],
) -> None:
    debug_html_path.parent.mkdir(parents=True, exist_ok=True)
    debug_json_path.parent.mkdir(parents=True, exist_ok=True)
    debug_screenshot_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        debug_html_path.write_text(page.content(), encoding="utf-8")
    except Exception as exc:
        extra["html_error"] = str(exc)

    try:
        page.screenshot(path=str(debug_screenshot_path), full_page=True)
    except Exception as exc:
        extra["screenshot_error"] = str(exc)

    try:
        debug_json_path.write_text(json.dumps(extra, indent=2), encoding="utf-8")
    except Exception:
        pass


def county_overlap_score(payload: Dict[str, Any]) -> int:
    map_counties = payload.get("mapCounties", {})
    if not isinstance(map_counties, dict):
        return 0
    keys = {str(k).upper() for k in map_counties.keys()}
    return len(keys & PA_COUNTIES)


def choose_pa_payload(payloads: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best_payload = None
    best_score = -1

    for payload in payloads:
        score = county_overlap_score(payload)
        if score > best_score:
            best_score = score
            best_payload = payload

    if best_score <= 0:
        return None
    return best_payload


def flatten_payload(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    map_counties = payload.get("mapCounties", {})
    if not isinstance(map_counties, dict):
        return [], []

    municipality_items: List[Dict[str, Any]] = []
    county_rows: List[Dict[str, Any]] = []

    for county_key, county_obj in map_counties.items():
        county_name = clean_text(county_obj.get("name") or county_key.title())
        area_data = county_obj.get("areaData", {}) or {}

        county_customers_out = safe_int(area_data.get("customerOutages")) or 0
        county_customers_served = safe_int(area_data.get("totalCustomers"))
        county_percent_out = safe_float(area_data.get("percentOut"))
        county_etr = clean_text(area_data.get("estimatedTimeRestored")) or None
        county_cause = clean_text(area_data.get("cause")) or None
        county_message = clean_text(area_data.get("message")) or None
        county_start = clean_text(area_data.get("earliestStart")) or None

        county_rows.append(
            {
                "county": county_name,
                "customers_out": county_customers_out,
                "customers_served": county_customers_served,
                "percent_out": county_percent_out,
                "etr": county_etr,
                "cause": county_cause,
                "message": county_message,
                "earliest_start": county_start,
            }
        )

        map_towns = county_obj.get("mapTowns", {}) or {}
        if not isinstance(map_towns, dict):
            continue

        for town_key, town_obj in map_towns.items():
            town_name = clean_text(town_obj.get("name") or town_key.title())
            town_data = town_obj.get("areaData", {}) or {}

            municipality_items.append(
                {
                    "municipality": town_name,
                    "county": county_name,
                    "state": "PA",
                    "company": None,
                    "customers_out": safe_int(town_data.get("customerOutages")) or 0,
                    "customers_served": safe_int(town_data.get("totalCustomers")),
                    "percent_out": safe_float(town_data.get("percentOut")),
                    "etr": clean_text(town_data.get("estimatedTimeRestored")) or None,
                    "cause": clean_text(town_data.get("cause")) or None,
                    "earliest_start": clean_text(town_data.get("earliestStart")) or None,
                    "message": clean_text(town_data.get("message")) or None,
                }
            )

    county_rows.sort(key=lambda x: (-x["customers_out"], x["county"].lower()))
    municipality_items.sort(
        key=lambda x: (-x["customers_out"], x["county"].lower(), x["municipality"].lower())
    )

    return municipality_items, county_rows


def scrape(
    show_browser: bool = False,
    debug_html: str = DEFAULT_DEBUG_HTML,
    debug_json: str = DEFAULT_DEBUG_JSON,
    debug_screenshot: str = DEFAULT_DEBUG_SCREENSHOT,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    debug_html_path = Path(debug_html)
    debug_json_path = Path(debug_json)
    debug_screenshot_path = Path(debug_screenshot)

    network_responses: List[Dict[str, Any]] = []
    captured_payloads: List[Dict[str, Any]] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not show_browser,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        context = browser.new_context(
            viewport={"width": 1600, "height": 2400},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )

        page = context.new_page()

        def handle_response(response) -> None:
            url = response.url
            if AREA_SEARCH_ENDPOINT not in url:
                return

            entry: Dict[str, Any] = {
                "url": url,
                "status": response.status,
            }

            try:
                entry["content_type"] = response.headers.get("content-type")
            except Exception:
                pass

            try:
                text = response.text()
                entry["body_preview"] = text[:2000]
                payload = json.loads(text)
                captured_payloads.append(payload)
                entry["county_overlap_score"] = county_overlap_score(payload)
                map_counties = payload.get("mapCounties", {})
                if isinstance(map_counties, dict):
                    entry["county_keys_preview"] = list(map_counties.keys())[:20]
                    entry["county_count"] = len(map_counties)
            except Exception as exc:
                entry["parse_error"] = str(exc)

            network_responses.append(entry)

        page.on("response", handle_response)

        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(8000)

            for selector in [
                "button:has-text('CLOSE')",
                "button:has-text('Close')",
                "text=CLOSE",
                "text=Close",
                "[aria-label='Close']",
            ]:
                try:
                    loc = page.locator(selector).first
                    if loc.is_visible(timeout=1000):
                        loc.click(timeout=1000)
                        page.wait_for_timeout(1500)
                        break
                except Exception:
                    pass

            for selector in [
                "a:has-text('NY & PA')",
                "button:has-text('NY & PA')",
                "text='NY & PA'",
            ]:
                try:
                    loc = page.locator(selector).first
                    if loc.is_visible(timeout=1000):
                        loc.click(timeout=1000)
                        page.wait_for_timeout(3000)
                        break
                except Exception:
                    pass

            page.wait_for_timeout(10000)

            extracted = page.evaluate(
                """
                () => {
                  function txt(el) {
                    return (el && el.innerText ? el.innerText : "")
                      .replace(/\\s+/g, " ")
                      .trim();
                  }

                  return {
                    title: document.title,
                    url_seen_in_browser: window.location.href,
                    headings: Array.from(document.querySelectorAll("h1,h2,h3,h4")).map(txt).filter(Boolean),
                    body_text_sample: txt(document.body).slice(0, 12000)
                  };
                }
                """
            )

            pa_payload = choose_pa_payload(captured_payloads)

            if not pa_payload:
                debug_payload = {
                    "fetched_at": iso_utc_now(),
                    "page_url": PAGE_URL,
                    "reason": "No Pennsylvania payload identified",
                    "title": extracted.get("title"),
                    "url_seen_in_browser": extracted.get("url_seen_in_browser"),
                    "headings": extracted.get("headings"),
                    "body_text_sample": extracted.get("body_text_sample"),
                    "network_responses": network_responses,
                }
                write_debug_files(
                    page,
                    debug_html_path=debug_html_path,
                    debug_json_path=debug_json_path,
                    debug_screenshot_path=debug_screenshot_path,
                    extra=debug_payload,
                )
                raise RuntimeError("No Pennsylvania outage payload found")

            items, county_rows = flatten_payload(pa_payload)

            debug_payload = {
                "fetched_at": iso_utc_now(),
                "page_url": PAGE_URL,
                "reason": "Successful PA payload capture",
                "title": extracted.get("title"),
                "url_seen_in_browser": extracted.get("url_seen_in_browser"),
                "headings": extracted.get("headings"),
                "network_responses": network_responses,
                "captured_payload_count": len(captured_payloads),
                "selected_pa_county_overlap_score": county_overlap_score(pa_payload),
                "selected_pa_county_count": len((pa_payload.get("mapCounties") or {})),
                "selected_pa_county_keys_preview": list((pa_payload.get("mapCounties") or {}).keys())[:25],
                "municipality_item_count": len(items),
                "county_row_count": len(county_rows),
            }
            write_debug_files(
                page,
                debug_html_path=debug_html_path,
                debug_json_path=debug_json_path,
                debug_screenshot_path=debug_screenshot_path,
                extra=debug_payload,
            )

            browser.close()
            return items, county_rows, pa_payload

        except PlaywrightTimeoutError as exc:
            write_debug_files(
                page,
                debug_html_path=debug_html_path,
                debug_json_path=debug_json_path,
                debug_screenshot_path=debug_screenshot_path,
                extra={
                    "fetched_at": iso_utc_now(),
                    "page_url": PAGE_URL,
                    "reason": "Playwright timeout",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                    "network_responses": network_responses,
                },
            )
            browser.close()
            raise

        except Exception as exc:
            write_debug_files(
                page,
                debug_html_path=debug_html_path,
                debug_json_path=debug_json_path,
                debug_screenshot_path=debug_screenshot_path,
                extra={
                    "fetched_at": iso_utc_now(),
                    "page_url": PAGE_URL,
                    "reason": "Unhandled scrape exception",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                    "network_responses": network_responses,
                },
            )
            browser.close()
            raise


def load_previous(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    items = data.get("items", [])
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


def apply_changes(
    current: List[Dict[str, Any]],
    previous: Dict[str, Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    summary = {"new": 0, "increasing": 0, "decreasing": 0, "unchanged": 0, "restored": 0}
    results: List[Dict[str, Any]] = []
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

        enriched = dict(item)
        enriched["previous_customers_out"] = prev_out
        enriched["change"] = change
        enriched["status"] = status
        results.append(enriched)

    for key, prev in previous.items():
        if key not in seen and (safe_int(prev.get("customers_out")) or 0) > 0:
            results.append(
                {
                    "municipality": prev.get("municipality", ""),
                    "county": prev.get("county", ""),
                    "state": "PA",
                    "company": None,
                    "customers_out": 0,
                    "customers_served": safe_int(prev.get("customers_served")),
                    "percent_out": 0.0,
                    "etr": None,
                    "cause": None,
                    "earliest_start": None,
                    "message": "Not present in current payload; treated as restored.",
                    "previous_customers_out": safe_int(prev.get("customers_out")) or 0,
                    "change": -(safe_int(prev.get("customers_out")) or 0),
                    "status": "restored",
                }
            )
            summary["restored"] += 1

    results.sort(
        key=lambda x: (
            {"new": 0, "increasing": 1, "decreasing": 2, "restored": 3, "unchanged": 4}.get(x.get("status", ""), 9),
            -(x.get("customers_out") or 0),
            x.get("county", "").lower(),
            x.get("municipality", "").lower(),
        )
    )

    return results, summary


def build_county_summary(tracked_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    county_map: Dict[str, Dict[str, Any]] = {}

    for item in tracked_items:
        county = clean_text(item.get("county")) or "Unknown"
        entry = county_map.setdefault(
            county,
            {
                "county": county,
                "customers_out": 0,
                "customers_served": 0,
                "change": 0,
                "municipalities": 0,
                "new": 0,
                "increasing": 0,
                "decreasing": 0,
                "unchanged": 0,
                "restored": 0,
            },
        )

        entry["customers_out"] += safe_int(item.get("customers_out")) or 0
        entry["customers_served"] += safe_int(item.get("customers_served")) or 0
        entry["change"] += safe_int(item.get("change")) or 0
        entry["municipalities"] += 1

        status = item.get("status")
        if status in entry:
            entry[status] += 1

    out = []
    for entry in county_map.values():
        served = entry["customers_served"]
        entry["percent_out"] = round((entry["customers_out"] / served) * 100, 2) if served else None
        out.append(entry)

    out.sort(key=lambda x: (-x["customers_out"], x["county"].lower()))
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--previous", default=DEFAULT_PREVIOUS)
    parser.add_argument("--show-browser", action="store_true")
    parser.add_argument("--debug-html", default=DEFAULT_DEBUG_HTML)
    parser.add_argument("--debug-json", default=DEFAULT_DEBUG_JSON)
    parser.add_argument("--debug-screenshot", default=DEFAULT_DEBUG_SCREENSHOT)
    args = parser.parse_args()

    output = Path(args.output)
    previous = Path(args.previous)

    output.parent.mkdir(parents=True, exist_ok=True)
    previous.parent.mkdir(parents=True, exist_ok=True)

    if output.exists():
        shutil.copyfile(output, previous)

    previous_lookup = load_previous(previous)

    current_items, raw_county_rows, _pa_payload = scrape(
        show_browser=args.show_browser,
        debug_html=args.debug_html,
        debug_json=args.debug_json,
        debug_screenshot=args.debug_screenshot,
    )

    tracked_items, summary = apply_changes(current_items, previous_lookup)
    county_summary = build_county_summary(tracked_items)

    total_customers_out = sum((safe_int(i.get("customers_out")) or 0) for i in tracked_items)
    active_count = sum(1 for i in tracked_items if (safe_int(i.get("customers_out")) or 0) > 0)

    result = {
        "name": "firstenergy_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source_page": PAGE_URL,
        "source_endpoint": AREA_SEARCH_ENDPOINT,
        "count": len(tracked_items),
        "active_count": active_count,
        "total_customers_out": total_customers_out,
        "summary": summary,
        "county_summary": county_summary,
        "raw_county_rows": raw_county_rows,
        "items": tracked_items,
    }

    output.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"Wrote {len(tracked_items)} municipality rows to {output}")
    print(f"Total customers out: {total_customers_out}")
    print("Summary:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
