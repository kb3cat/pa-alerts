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
DEBUG_HTML = "data/power_outages_ppl_debug.html"
DEBUG_PNG = "data/power_outages_ppl_debug.png"


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


def fetch():
    Path("data").mkdir(parents=True, exist_ok=True)

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
            page.wait_for_timeout(10000)

            # Try to activate tabular/county view first.
            click_result = page.evaluate("""
                () => {
                    const patterns = [
                        /view outages by county/i,
                        /by county/i,
                        /tabular/i
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

            page.wait_for_timeout(8000)

            # Browser-context request with browser-ish headers.
            result = page.evaluate(f"""
                async () => {{
                    try {{
                        const res = await fetch("{TABULAR_URL}", {{
                            method: "GET",
                            credentials: "include",
                            headers: {{
                                "Accept": "application/json, text/plain, */*",
                                "X-Requested-With": "XMLHttpRequest"
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
                    "fetched_at": iso_utc_now(),
                    "reason": "fetch failed",
                    "click_result": click_result,
                    "result": result,
                    "page_url": page.url,
                    "title": page.title(),
                    "body_sample": page.evaluate("() => document.body.innerText.slice(0, 3000)")
                }, page=page)
                browser.close()
                raise RuntimeError("PPL fetch failed")

            text = result.get("text", "")
            content_type = result.get("content_type", "")
            status = result.get("status")

            try:
                data = json.loads(text)
            except Exception:
                write_debug({
                    "fetched_at": iso_utc_now(),
                    "reason": "response not json",
                    "click_result": click_result,
                    "status": status,
                    "content_type": content_type,
                    "preview": text[:3000],
                    "page_url": page.url,
                    "title": page.title(),
                    "body_sample": page.evaluate("() => document.body.innerText.slice(0, 3000)")
                }, page=page)
                browser.close()
                raise RuntimeError("Response not JSON")

            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "successful ppl capture",
                "click_result": click_result,
                "status": status,
                "content_type": content_type,
                "top_level_keys": list(data.keys()) if isinstance(data, dict) else [],
                "page_url": page.url,
                "title": page.title()
            }, page=page)

            browser.close()
            return data

        except RuntimeError:
            browser.close()
            raise

        except Exception as e:
            write_debug({
                "fetched_at": iso_utc_now(),
                "reason": "exception",
                "error": str(e),
                "traceback": traceback.format_exc()
            }, page=page)
            browser.close()
            raise


def transform(raw):
    counties = []

    for row in raw.get("data", []):
        county = row.get("nm")
        if not county:
            continue

        counties.append({
            "county": county,
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
        "customers_served_total": raw.get("cc"),
        "percent_out_total": parse_percent(raw.get("pc")),
        "source_last_updated": raw.get("dt"),
        "county_summary": counties
    }


def main():
    raw = fetch()
    parsed = transform(raw)

    Path("data").mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)

    print("PPL SUCCESS")
    print(f"Counties: {len(parsed['county_summary'])}")


if __name__ == "__main__":
    main()
