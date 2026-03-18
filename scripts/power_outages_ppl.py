#!/usr/bin/env python3

import json
import traceback
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

PAGE_URL = "https://omap.prod.pplweb.com/omap#pg-tabular"
OUTPUT_FILE = "data/power_outages_ppl.json"
DEBUG_FILE = "data/power_outages_ppl_debug.json"


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_percent(value):
    if value is None:
        return None
    text = str(value).replace("%", "").replace("<", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def write_debug(payload):
    Path("data").mkdir(parents=True, exist_ok=True)
    with open(DEBUG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def fetch():
    Path("data").mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            viewport={"width": 1600, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        network_log = []
        captured = {"text": None, "url": None, "status": None, "content_type": None}

        def log_response(resp):
            url = resp.url
            if "omap.prod.pplweb.com/omap/" in url:
                entry = {
                    "url": url,
                    "status": resp.status,
                }
                try:
                    entry["content_type"] = resp.headers.get("content-type")
                except Exception:
                    pass
                network_log.append(entry)

            if "omap.prod.pplweb.com/omap/Tabular?opco=PA" in url and resp.status == 200:
                try:
                    captured["text"] = resp.text()
                    captured["url"] = url
                    captured["status"] = resp.status
                    captured["content_type"] = resp.headers.get("content-type", "")
                except Exception as exc:
                    captured["text"] = None
                    captured["url"] = url
                    captured["status"] = resp.status
                    captured["content_type"] = ""
                    network_log.append({
                        "url": url,
                        "status": resp.status,
                        "capture_error": str(exc),
                    })

        page.on("response", log_response)

        try:
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)

            # Give the app time to initialize and make its own calls
            page.wait_for_timeout(15000)

            if not captured["text"]:
                write_debug({
                    "fetched_at": iso_utc_now(),
                    "reason": "No Tabular response captured",
                    "network_log": network_log,
                })
                browser.close()
                raise RuntimeError("No PPL Tabular response captured")

            text = captured["text"]

            try:
                data = json.loads(text)
            except Exception:
                write_debug({
                    "fetched_at": iso_utc_now(),
                    "reason": "Tabular response was not JSON",
                    "response_url": captured["url"],
                    "status": captured["status"],
                    "content_type": captured["content_type"],
                    "preview": text[:2000],
                    "network_log": network_log,
                })
                browser.close()
                raise RuntimeError("PPL Tabular response was not JSON")

            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "Successful PPL capture",
                "response_url": captured["url"],
                "status": captured["status"],
                "content_type": captured["content_type"],
                "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
                "network_log": network_log[-25:],
            })

            browser.close()
            return data

        except PlaywrightTimeoutError as exc:
            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "Timed out loading PPL page",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "network_log": network_log,
            })
            browser.close()
            raise

        except Exception as exc:
            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "Unhandled PPL exception",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "network_log": network_log,
            })
            try:
                browser.close()
            except Exception:
                pass
            raise


def transform(raw):
    counties = []

    for row in raw.get("data", []):
        county = row.get("nm")
        if not county:
            continue

        counties.append({
            "county": county,
            "customers_out": row.get("nc", 0),
            "customers_served": row.get("tc", 0),
            "percent_out": parse_percent(row.get("p")),
            "source": "PPL",
        })

    counties.sort(key=lambda x: x["customers_out"], reverse=True)

    return {
        "name": "ppl_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source": "PPL",
        "source_last_updated": raw.get("dt"),
        "total_customers_out": raw.get("nc", 0),
        "customers_served_total": raw.get("cc", 0),
        "percent_out_total": parse_percent(raw.get("pc")),
        "county_summary": counties,
    }


def main():
    raw = fetch()
    parsed = transform(raw)

    Path("data").mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)

    print(f"Wrote {OUTPUT_FILE}")
    print(f"Total customers out: {parsed['total_customers_out']}")
    print(f"County rows: {len(parsed['county_summary'])}")


if __name__ == "__main__":
    main()
