import json
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

URL = "https://omap.prod.pplweb.com/omap#pg-tabular"


def fetch():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto(URL, timeout=60000)
        page.wait_for_timeout(5000)

        # Pull the API directly from browser context
        data = page.evaluate("""
            async () => {
                const res = await fetch("/omap/Tabular?opco=PA");
                return await res.json();
            }
        """)

        browser.close()
        return data


def transform(raw):
    results = []

    for row in raw.get("data", []):
        results.append({
            "county": row.get("nm"),
            "customers_out": row.get("nc", 0),
            "total_customers": row.get("tc", 0),
        })

    return results


def main():
    raw = fetch()

    data = transform(raw)

    output = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "PPL",
        "count": len(data),
        "data": data,
    }

    with open("data/power_outages_ppl.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {len(data)} PPL county rows")


if __name__ == "__main__":
    main()
