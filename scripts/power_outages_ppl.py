#!/usr/bin/env python3

import json
from datetime import datetime, timezone

import requests

URL = "https://omap.prod.pplweb.com/omap/Tabular?opco=PA"
OUTPUT_FILE = "data/power_outages_ppl.json"

HEADERS = {
    "User-Agent": "PA-Alerts-PPL"
}


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


def fetch():
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://omap.prod.pplweb.com/omap",
        "Origin": "https://omap.prod.pplweb.com"
    }

    r = requests.get(URL, headers=headers, timeout=30)

    # DEBUG (super important for this API)
    if not r.text.strip():
        raise RuntimeError("Empty response from PPL")

    try:
        return r.json()
    except Exception:
        print("Non-JSON response received:")
        print(r.text[:500])
        raise


def parse(data):
    counties = []

    for row in data.get("data", []):
        counties.append({
            "county": row.get("nm"),
            "customers_out": row.get("nc", 0),
            "customers_served": row.get("tc", 0),
            "percent_out": parse_percent(row.get("p")),
            "source": "PPL"
        })

    counties = [c for c in counties if c["county"]]
    counties.sort(key=lambda x: x["customers_out"], reverse=True)

    return {
        "name": "ppl_power_outages_pa",
        "fetched_at": iso_utc_now(),
        "source": "PPL",
        "source_last_updated": data.get("dt"),
        "total_customers_out": data.get("nc", 0),
        "customers_served_total": data.get("cc", 0),
        "percent_out_total": parse_percent(data.get("pc")),
        "county_summary": counties
    }


def main():
    raw = fetch()
    parsed = parse(raw)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(parsed, f, indent=2)

    print(f"Wrote {OUTPUT_FILE}")
    print(f"Total customers out: {parsed['total_customers_out']}")
    print(f"County rows: {len(parsed['county_summary'])}")


if __name__ == "__main__":
    main()
