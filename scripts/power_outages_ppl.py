#!/usr/bin/env python3

import json
import traceback
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

PAGE_URL = "https://omap.prod.pplweb.com/omap"
TABULAR_URL = "https://omap.prod.pplweb.com/omap/Tabular?opco=PA"

OUTPUT_FILE = "data/power_outages_ppl.json"
DEBUG_FILE = "data/power_outages_ppl_debug.json"


def iso_utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_debug(payload):
    Path("data").mkdir(parents=True, exist_ok=True)
    with open(DEBUG_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def parse_percent(v):
    try:
        return float(str(v).replace("%", "").replace("<", "").strip())
    except:
        return None


def fetch():
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )

        page = context.new_page()

        try:
            # 🔑 KEY FIX: DO NOT USE networkidle
            page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=60000)

            # Let JS app fully initialize
            page.wait_for_timeout(10000)

            # Force tabular route
            page.evaluate("""
                () => {
                    window.location.hash = "#pg-tabular";
                    window.dispatchEvent(new HashChangeEvent("hashchange"));
                }
            """)

            page.wait_for_timeout(8000)

            # 🔥 Direct fetch from inside browser session
            result = page.evaluate(f"""
                async () => {{
                    try {{
                        const res = await fetch("{TABULAR_URL}", {{
                            headers: {{
                                "Accept": "application/json, text/plain, */*"
                            }}
                        }});

                        const text = await res.text();

                        return {{
                            ok: true,
                            status: res.status,
                            content_type: res.headers.get("content-type"),
                            text: text
                        }};
                    }} catch (e) {{
                        return {{
                            ok: false,
                            error: String(e)
                        }};
                    }}
                }}
            """)

            if not result.get("ok"):
                write_debug({
                    "reason": "fetch failed",
                    "result": result
                })
                raise RuntimeError("PPL fetch failed")

            text = result.get("text", "")

            try:
                data = json.loads(text)
            except:
                write_debug({
                    "reason": "not JSON",
                    "preview": text[:2000],
                    "status": result.get("status"),
                    "content_type": result.get("content_type")
                })
                raise RuntimeError("Response not JSON")

            browser.close()
            return data

        except Exception as e:
            write_debug({
                "reason": "exception",
                "error": str(e),
                "traceback": traceback.format_exc()
            })
            browser.close()
            raise


def transform(raw):
    counties = []

    for row in raw.get("data", []):
        counties.append({
            "county": row.get("nm"),
            "customers_out": row.get("nc"),
            "customers_served": row.get("tc"),
            "percent_out": parse_percent(row.get("p")),
            "source": "PPL"
        })

    counties.sort(key=lambda x: x["customers_out"] or 0, reverse=True)

    return {
        "name": "ppl_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "total_customers_out": raw.get("nc"),
        "county_summary": counties
    }


def main():
    raw = fetch()
    parsed = transform(raw)

    Path("data").mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(parsed, f, indent=2)

    print("PPL SUCCESS")
    print(f"Counties: {len(parsed['county_summary'])}")


if __name__ == "__main__":
    main()
