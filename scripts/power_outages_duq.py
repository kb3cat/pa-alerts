#!/usr/bin/env python3

import json
from pathlib import Path
from playwright.sync_api import sync_playwright


URL = "https://dlc.datacapable.com/map/?disableLinks=&utm_source=chatgpt.com"
OUTPUT_FILE = Path("data/power_outages_duq.json")


def to_int(value):
    try:
        if value is None:
            return 0
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def normalize_name(value):
    return str(value or "").strip().upper()


def calc_percent(out_val, served_val):
    out_num = to_int(out_val)
    served_num = to_int(served_val)
    if served_num <= 0:
        return None
    return round((out_num / served_num) * 100, 2)


def build_output(events_data, count_data):
    events = events_data if isinstance(events_data, list) else []
    counts = count_data if isinstance(count_data, list) else []

    items = []
    counties_from_items = {}
    municipalities_from_items = {}

    for e in events:
        props = e.get("additionalProperties") or []

        def get_prop(key):
            for p in props:
                if p.get("property") == key:
                    return p.get("value") or []
            return []

        counties = get_prop("counties")
        municipalities = get_prop("municipalities")
        zips = get_prop("zips")

        county = normalize_name(counties[0] if counties else "UNKNOWN")
        municipality = normalize_name(municipalities[0] if municipalities else "UNKNOWN")
        zip_code = str(zips[0] if zips else "UNKNOWN").strip()

        outages = to_int(e.get("numPeople"))

        item = {
            "utility": "Duquesne Light",
            "outages": outages,
            "status": e.get("status") or "Unknown",
            "county": county,
            "municipality": municipality,
            "zip": zip_code,
            "lat": e.get("latitude"),
            "lon": e.get("longitude"),
        }
        items.append(item)

        counties_from_items[county] = counties_from_items.get(county, 0) + outages
        municipalities_from_items[municipality] = municipalities_from_items.get(municipality, 0) + outages

    county_summary = []
    municipality_summary = []
    zip_summary = []

    for row in counts:
        row_type = str(row.get("type") or "").upper()
        name = str(row.get("name") or "").strip()
        customers_affected = to_int(row.get("customersAffected"))
        customers_served = to_int(row.get("customersServed"))

        if not name or not row_type:
            continue

        if row_type == "COUNTY":
            county_summary.append({
                "county": normalize_name(name),
                "customers_out": customers_affected,
                "customers_served": customers_served,
                "percent_out": calc_percent(customers_affected, customers_served),
            })
        elif row_type == "MUNICIPALITY":
            municipality_summary.append({
                "municipality": normalize_name(name),
                "customers_out": customers_affected,
                "customers_served": customers_served,
                "percent_out": calc_percent(customers_affected, customers_served),
            })
        elif row_type == "ZIP":
            zip_summary.append({
                "zip": name,
                "customers_out": customers_affected,
                "customers_served": customers_served,
                "percent_out": calc_percent(customers_affected, customers_served),
            })

    county_summary.sort(key=lambda x: (-to_int(x["customers_out"]), x["county"]))
    municipality_summary.sort(key=lambda x: (-to_int(x["customers_out"]), x["municipality"]))
    zip_summary.sort(key=lambda x: (-to_int(x["customers_out"]), x["zip"]))

    total_outages = sum(to_int(i.get("outages")) for i in items)

    output = {
        "name": "power_outages_duq",
        "utility": "Duquesne Light",
        "fetched_at": None,
        "total_outages": total_outages,
        "counties": counties_from_items,
        "municipalities": municipalities_from_items,
        "county_summary": county_summary,
        "municipality_summary": municipality_summary,
        "zip_summary": zip_summary,
        "raw_count": len(items),
        "items": items,
    }

    return output


def main():
    events_data = None
    count_data = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1600, "height": 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        )
        page = context.new_page()

        def handle_response(response):
            nonlocal events_data, count_data
            url = response.url

            try:
                if "/datacapable/v2/p/dlc/map/events" in url:
                    data = response.json()
                    if isinstance(data, list):
                        events_data = data

                if "/datacapable/v2/p/dlc/map/count?types=ZIP,COUNTY,MUNICIPALITY" in url:
                    data = response.json()
                    if isinstance(data, list):
                        count_data = data
            except Exception:
                pass

        page.on("response", handle_response)

        page.goto(URL, wait_until="networkidle", timeout=120000)

        try:
            page.locator("text=COUNTY").click(timeout=10000)
        except Exception:
            pass

       page.wait_for_response(
    lambda r: "count?types=ZIP,COUNTY,MUNICIPALITY" in r.url,
    timeout=15000
)

        if events_data is None:
            try:
                events_data = page.evaluate(
                    """
                    async () => {
                      const r = await fetch("https://utilisocial.io/datacapable/v2/p/dlc/map/events", {
                        credentials: "include"
                      });
                      return await r.json();
                    }
                    """
                )
            except Exception:
                events_data = []

        if count_data is None:
            try:
                count_data = page.evaluate(
                    """
                    async () => {
                      const r = await fetch("https://utilisocial.io/datacapable/v2/p/dlc/map/count?types=ZIP,COUNTY,MUNICIPALITY", {
                        credentials: "include"
                      });
                      return await r.json();
                    }
                    """
                )
            except Exception:
                count_data = []

        fetched_at = page.evaluate("() => new Date().toISOString()")

        browser.close()

    if not isinstance(events_data, list):
        raise RuntimeError("Failed to capture Duquesne events data")

    if not isinstance(count_data, list):
        raise RuntimeError("Failed to capture Duquesne count data")

    output = build_output(events_data, count_data)
    output["fetched_at"] = fetched_at

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {OUTPUT_FILE}")
    print(f"Duquesne total outages: {output['total_outages']}")
    print(f"County rows: {len(output['county_summary'])}")
    print(f"Municipality rows: {len(output['municipality_summary'])}")
    print(f"ZIP rows: {len(output['zip_summary'])}")


if __name__ == "__main__":
    main()
