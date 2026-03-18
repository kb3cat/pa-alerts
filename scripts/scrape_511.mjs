#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import traceback
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
DEFAULT_DEBUG_HTML = "data/power_outages_debug.html"
DEFAULT_DEBUG_JSON = "data/power_outages_debug.json"
DEFAULT_DEBUG_SCREENSHOT = "data/power_outages_debug.png"


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
    text = str(value).replace("%", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def clean_text(v: Any) -> str:
    return re.sub(r"\s+", " ", str(v or "")).strip()


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
        html = page.content()
        debug_html_path.write_text(html, encoding="utf-8")
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


def scrape(
    show_browser: bool = False,
    debug_html: str = DEFAULT_DEBUG_HTML,
    debug_json: str = DEFAULT_DEBUG_JSON,
    debug_screenshot: str = DEFAULT_DEBUG_SCREENSHOT,
) -> List[Dict[str, Any]]:
    debug_html_path = Path(debug_html)
    debug_json_path = Path(debug_json)
    debug_screenshot_path = Path(debug_screenshot)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=not show_browser,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        page = browser.new_page(viewport={"width": 1600, "height": 2400})

        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(8000)

            for selector in [
                "button:has-text('CLOSE')",
                "button:has-text('Close')",
                "text=CLOSE",
                "text=Close",
                "[aria-label='Close']",
                ".modal button",
                ".fe-dialog button",
            ]:
                try:
                    loc = page.locator(selector).first
                    if loc.is_visible(timeout=1500):
                        loc.click(timeout=1500)
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
                    if loc.is_visible(timeout=1500):
                        loc.click(timeout=1500)
                        page.wait_for_timeout(3000)
                        break
                except Exception:
                    pass

            page.wait_for_timeout(6000)

            extracted = page.evaluate(
                """
                () => {
                  function txt(el) {
                    return (el && el.innerText ? el.innerText : "")
                      .replace(/\\s+/g, " ")
                      .trim();
                  }

                  function lower(v) {
                    return (v || "").toLowerCase();
                  }

                  function rowFromCells(cells) {
                    const values = cells.map(txt).filter(Boolean);
                    if (values.length < 2) return null;

                    const joined = values.join(" | ");
                    if (/municipality|county|customers out|customers served|percent out|etr/i.test(joined)) {
                      return null;
                    }
                    if (/pennsylvania search|new york search|no outages/i.test(joined)) {
                      return null;
                    }

                    return {
                      municipality: values[0] || "",
                      county: values[1] || "",
                      customers_out: values[2] || "",
                      customers_served: values[3] || "",
                      percent_out: values[4] || "",
                      etr: values[5] || "",
                      raw_cells: values
                    };
                  }

                  function tableRows(table) {
                    const rows = [];
                    const trs = Array.from(table.querySelectorAll("tr"));
                    for (const tr of trs) {
                      const cells = Array.from(tr.querySelectorAll("td,th"));
                      const parsed = rowFromCells(cells);
                      if (parsed) rows.push(parsed);
                    }
                    return rows;
                  }

                  function looksUseful(row) {
                    const muni = lower(row.municipality);
                    const county = lower(row.county);
                    if (!muni && !county) return false;
                    if (muni.includes("customers out") || county.includes("customers out")) return false;
                    if (muni.includes("county") && county.includes("municipality")) return false;
                    return true;
                  }

                  const result = {
                    title: document.title,
                    url: window.location.href,
                    headings: Array.from(document.querySelectorAll("h1,h2,h3,h4")).map(txt).filter(Boolean),
                    table_count: document.querySelectorAll("table").length,
                    candidate_rows: [],
                    body_text_sample: txt(document.body).slice(0, 8000)
                  };

                  const allTables = Array.from(document.querySelectorAll("table"));
                  for (const table of allTables) {
                    const rows = tableRows(table);
                    for (const row of rows) {
                      if (looksUseful(row)) result.candidate_rows.push(row);
                    }
                  }

                  return result;
                }
                """
            )

            rows = extracted.get("candidate_rows", [])

            cleaned: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue

                municipality = clean_text(row.get("municipality"))
                county = clean_text(row.get("county"))

                if not municipality and not county:
                    continue

                joined = " ".join(
                    [
                        municipality,
                        county,
                        clean_text(row.get("customers_out")),
                        clean_text(row.get("customers_served")),
                        clean_text(row.get("percent_out")),
                        clean_text(row.get("etr")),
                    ]
                ).lower()

                if any(
                    bad in joined
                    for bad in [
                        "pennsylvania search",
                        "new york search",
                        "customers out",
                        "customers served",
                        "municipality",
                        "county",
                    ]
                ):
                    continue

                cleaned.append(
                    {
                        "municipality": municipality,
                        "county": county,
                        "customers_out": safe_int(row.get("customers_out")) or 0,
                        "customers_served": safe_int(row.get("customers_served")),
                        "percent_out": safe_float(row.get("percent_out")),
                        "etr": clean_text(row.get("etr")) or None,
                    }
                )

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

            if not deduped:
                debug_payload = {
                    "fetched_at": iso_utc_now(),
                    "page_url": PAGE_URL,
                    "title": extracted.get("title"),
                    "url_seen_in_browser": extracted.get("url"),
                    "headings": extracted.get("headings"),
                    "table_count": extracted.get("table_count"),
                    "candidate_rows_found": len(rows),
                    "candidate_rows_preview": rows[:25],
                    "body_text_sample": extracted.get("body_text_sample", ""),
                    "reason": "No usable outage rows found",
                }
                write_debug_files(
                    page,
                    debug_html_path=debug_html_path,
                    debug_json_path=debug_json_path,
                    debug_screenshot_path=debug_screenshot_path,
                    extra=debug_payload,
                )
                raise RuntimeError(
                    f"No outage rows found. Wrote debug files: "
                    f"{debug_html_path}, {debug_json_path}, {debug_screenshot_path}"
                )

            browser.close()
            return deduped

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
                },
            )
            browser.close()
            raise RuntimeError(f"Timed out loading FE page: {exc}") from exc

        except Exception as exc:
            try:
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
                    },
                )
            except Exception:
                pass
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
                    "customers_out": 0,
                    "customers_served": safe_int(prev.get("customers_served")),
                    "percent_out": 0.0,
                    "etr": None,
                    "previous_customers_out": safe_int(prev.get("customers_out")) or 0,
                    "change": -(safe_int(prev.get("customers_out")) or 0),
                    "status": "restored",
                }
            )
            summary["restored"] += 1

    return results, summary


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

    prev_data = load_previous(previous)
    current = scrape(
        show_browser=args.show_browser,
        debug_html=args.debug_html,
        debug_json=args.debug_json,
        debug_screenshot=args.debug_screenshot,
    )

    items, summary = apply_changes(current, prev_data)

    result = {
        "fetched_at": iso_utc_now(),
        "count": len(items),
        "summary": summary,
        "items": items,
    }

    output.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"Wrote {len(items)} rows to {output}")
    print("Summary:", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
