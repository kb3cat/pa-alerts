import json
from datetime import datetime, timezone
from playwright.sync_api import sync_playwright

URL = "https://omap.prod.pplweb.com/omap#pg-tabular"


def fetch():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        page.goto("https://omap.prod.pplweb.com/omap#pg-tabular", timeout=60000)

        # wait for page JS to initialize session
        page.wait_for_timeout(5000)

        data = page.evaluate("""
            async () => {
                const res = await fetch("https://omap.prod.pplweb.com/omap/Tabular?opco=PA", {
                    method: "GET",
                    headers: {
                        "Accept": "application/json, text/plain, */*"
                    }
                });

                if (!res.ok) {
                    return { error: "HTTP " + res.status };
                }

                const text = await res.text();

                try {
                    return JSON.parse(text);
                } catch (e) {
                    return { error: "NOT_JSON", preview: text.substring(0, 300) };
                }
            }
        """)

        browser.close()

        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"PPL fetch failed: {data}")

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
