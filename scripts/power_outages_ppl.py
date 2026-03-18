#!/usr/bin/env python3

import json
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

PAGE_URL = "https://omap.prod.pplweb.com/omap#pg-tabular"

OUTPUT_FILE = "data/power_outages_ppl.json"
DEBUG_FILE = "data/power_outages_ppl_debug.json"
DEBUG_HTML = "data/power_outages_ppl_debug.html"
DEBUG_PNG = "data/power_outages_ppl_debug.png"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def clean_text(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def safe_int(value):
    if value is None:
        return 0
    text = str(value).replace(",", "").strip()
    if not text:
        return 0
    m = re.search(r"-?\d+", text)
    return int(m.group(0)) if m else 0


def write_debug(payload, page=None):
    Path("data").mkdir(parents=True, exist_ok=True)

    if page is not None:
        try:
            Path(DEBUG_HTML).write_text(page.content(), encoding="utf-8")
        except Exception as exc:
            payload["html_error"] = str(exc)

        try:
            page.screenshot(path=DEBUG_PNG, full_page=True)
        except Exception as exc:
            payload["screenshot_error"] = str(exc)

    with open(DEBUG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def extract_table(page):
    return page.evaluate("""
        () => {
            function txt(el) {
                return (el && el.innerText ? el.innerText : "").replace(/\\s+/g, " ").trim();
            }

            const all = [];
            const tables = Array.from(document.querySelectorAll("table"));

            for (const table of tables) {
                const rows = Array.from(table.querySelectorAll("tr"));
                for (const tr of rows) {
                    const cells = Array.from(tr.querySelectorAll("td, th")).map(txt).filter(Boolean);
                    if (cells.length) all.push(cells);
                }
            }

            return {
                title: document.title,
                body_sample: txt(document.body).slice(0, 5000),
                table_count: tables.length,
                rows: all
            };
        }
    """)


def fetch():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1600, "height": 1200},
            locale="en-US",
        )

        page = context.new_page()

        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_timeout(12000)

            # Click county/tabular view if present
            click_result = page.evaluate("""
                () => {
                    const patterns = [
                        /view outages by county/i,
                        /by county/i,
                        /county & municipality/i
                    ];

                    const els = Array.from(document.querySelectorAll("a, button, div, span, li"));
                    for (const el of els) {
                        const t = (el.innerText || "").trim();
                        if (!t) continue;
                        if (patterns.some(rx => rx.test(t))) {
                            try {
                                el.click();
                                return { clicked: true, text: t };
                            } catch (e) {}
                        }
                    }

                    window.location.hash = "#pg-tabular";
                    window.dispatchEvent(new HashChangeEvent("hashchange"));
                    return { clicked: false, text: "forced hashchange" };
                }
            """)

            page.wait_for_timeout(10000)

            extracted = extract_table(page)
            rows = extracted.get("rows", [])

            if not rows:
                write_debug({
                    "fetched_at": iso_utc_now(),
                    "reason": "No table rows found",
                    "click_result": click_result,
                    "table_count": extracted.get("table_count"),
                    "title": extracted.get("title"),
                    "body_sample": extracted.get("body_sample"),
                }, page=page)
                browser.close()
                raise RuntimeError("No PPL table rows found")

            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "Successful PPL table capture",
                "click_result": click_result,
                "table_count": extracted.get("table_count"),
                "title": extracted.get("title"),
                "row_count": len(rows),
                "row_preview": rows[:20],
            }, page=page)

            browser.close()
            return extracted

        except Exception as e:
            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "exception",
                "error": str(e),
                "traceback": traceback.format_exc(),
            }, page=page)
            browser.close()
            raise


def transform(extracted):
    rows = extracted.get("rows", [])

    counties = []
    municipalities = []

    current_county = None

    for cells in rows:
        joined = " | ".join(cells).lower()

        # Skip headers / junk
        if any(x in joined for x in [
            "county & municipality",
            "customers affected",
            "customers served",
            "view on map",
            "outage information by county",
        ]):
            continue

        if len(cells) < 3:
            continue

        name = clean_text(cells[0])
        affected = safe_int(cells[1])
        served = safe_int(cells[2])

        # Heuristic:
        # county rows appear as CountyName + totals
        # municipality rows follow underneath
        is_county = (
            current_county is None
            or name.lower().endswith("county")
            or ("twp" not in name.lower() and "boro" not in name.lower() and "township" not in name.lower()
                and "city" not in name.lower() and "borough" not in name.lower()
                and "town" not in name.lower() and "village" not in name.lower()
                and served > 10000)
        )

        if is_county:
            current_county = name
            percent = round((affected / served) * 100, 2) if served else None
            counties.append({
                "county": name,
                "customers_out": affected,
                "customers_served": served,
                "percent_out": percent,
                "source": "PPL"
            })
        else:
            municipalities.append({
                "municipality": name,
                "county": current_county,
                "customers_out": affected,
                "customers_served": served,
                "percent_out": round((affected / served) * 100, 2) if served else None,
                "source": "PPL"
            })

    counties.sort(key=lambda x: x["customers_out"], reverse=True)
    municipalities.sort(key=lambda x: x["customers_out"], reverse=True)

    total_out = sum(c["customers_out"] for c in counties)

    return {
        "name": "ppl_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source": "PPL",
        "total_customers_out": total_out,
        "county_summary": counties,
        "items": municipalities
    }


def main():
    extracted = fetch()
    parsed = transform(extracted)

    Path("data").mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)

    print("PPL SUCCESS")
    print(f"County rows: {len(parsed['county_summary'])}")
    print(f"Municipality rows: {len(parsed['items'])}")


if __name__ == "__main__":
    main()
