#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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

    debug_json_path.write_text(json.dumps(extra, indent=2), encoding="utf-8")


def scrape(
    show_browser: bool = False,
    debug_html: str = DEFAULT_DEBUG_HTML,
    debug_json: str = DEFAULT_DEBUG_JSON,
    debug_screenshot: str = DEFAULT_DEBUG_SCREENSHOT,
) -> List[Dict[str, Any]]:
    debug_html_path = Path(debug_html)
    debug_json_path = Path(debug_json)
    debug_screenshot_path = Path(debug_screenshot)

    network_events: List[Dict[str, Any]] = []

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

        def log_request(request) -> None:
            url = request.url
            if "firstenergycorp.com" in url or "firstenergy" in url.lower():
                network_events.append(
                    {
                        "kind": "request",
                        "resource_type": request.resource_type,
                        "method": request.method,
                        "url": url,
                    }
                )

        def log_response(response) -> None:
            url = response.url
            if "firstenergycorp.com" in url or "firstenergy" in url.lower():
                entry: Dict[str, Any] = {
                    "kind": "response",
                    "status": response.status,
                    "url": url,
                }
                try:
                    headers = response.headers
                    entry["content_type"] = headers.get("content-type")
                except Exception:
                    pass

                try:
                    ctype = (entry.get("content_type") or "").lower()
                    if "json" in ctype:
                        text = response.text()
                        entry["body_preview"] = text[:2000]
                except Exception as exc:
                    entry["body_preview_error"] = str(exc)

                network_events.append(entry)

        page.on("request", log_request)
        page.on("response", log_response)

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
                    table_count: document.querySelectorAll("table").length,
                    headings: Array.from(document.querySelectorAll("h1,h2,h3,h4")).map(txt).filter(Boolean),
                    body_text_sample: txt(document.body).slice(0, 12000)
                  };
                }
                """
            )

            debug_payload = {
                "fetched_at": iso_utc_now(),
                "page_url": PAGE_URL,
                "reason": "Network inspection run",
                "title": extracted.get("title"),
                "url_seen_in_browser": extracted.get("url_seen_in_browser"),
                "table_count": extracted.get("table_count"),
                "headings": extracted.get("headings"),
                "body_text_sample": extracted.get("body_text_sample"),
                "network_events": network_events[-200:],
            }

            write_debug_files(
                page,
                debug_html_path=debug_html_path,
                debug_json_path=debug_json_path,
                debug_screenshot_path=debug_screenshot_path,
                extra=debug_payload,
            )

            browser.close()
            raise RuntimeError("Inspection run complete; check debug JSON for network events.")

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
                    "network_events": network_events[-200:],
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
                    "network_events": network_events[-200:],
                },
            )
            browser.close()
            raise


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

    scrape(
        show_browser=args.show_browser,
        debug_html=args.debug_html,
        debug_json=args.debug_json,
        debug_screenshot=args.debug_screenshot,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
