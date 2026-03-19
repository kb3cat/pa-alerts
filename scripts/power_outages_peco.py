#!/usr/bin/env python3

import json
from datetime import datetime, timezone
from pathlib import Path

import requests

TOTALS_URL = "https://kubra.io/data/e1548a52-76e1-405b-a27d-172feee110b9/public/summary-1/data.json"
REPORT_URL = "https://kubra.io/data/ee4132a9-7316-453a-b383-9e048a28a709/public/reports/a36a6292-1c55-44de-a6a9-44fedf9482ee_report.json"

OUTPUT_FILE = Path("data/power_outages_peco.json")


def iso_utc_now():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_json(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def to_int(v):
    try:
        if isinstance(v, dict):
            return int(v.get("val", 0))
        return int(v)
    except:
        return 0


def is_philly_zip(name):
    return name.isdigit() and name.startswith("19") and len(name) == 5


def main():
    totals = fetch_json(TOTALS_URL)
    report = fetch_json(REPORT_URL)

    file_data = report.get("file_data", {})

    counties = []
    municipalities = []

    philly_total = 0
    philly_served = 0
    philly_outages = 0

    for county in file_data.get("areas", []):
        county_name = county.get("name", "").title()

        counties.append({
            "name": county_name,
            "customers_affected": to_int(county.get("cust_a")),
            "customers_served": to_int(county.get("cust_s")),
            "percent_affected": county.get("percent_cust_a", {}).get("val"),
            "outages": to_int(county.get("n_out")),
            "etr": county.get("etr"),
        })

        for muni in county.get("areas", []):
            name_raw = muni.get("name", "")
            name = name_raw.title()

            cust = to_int(muni.get("cust_a"))
            served = to_int(muni.get("cust_s"))

            # Aggregate Philly ZIPs
            if county_name == "Philadelphia" and is_philly_zip(name_raw):
                philly_total += cust
                philly_served += served
                philly_outages += to_int(muni.get("n_out"))
                continue

            municipalities.append({
                "name": name,
                "county": county_name,
                "customers_affected": cust,
                "customers_served": served,
                "percent_affected": muni.get("percent_cust_a", {}).get("val"),
                "outages": to_int(muni.get("n_out")),
                "etr": muni.get("etr"),
            })

    # Add aggregated Philadelphia
    if philly_total > 0:
        percent = (philly_total / philly_served * 100) if philly_served else 0

        municipalities.append({
            "name": "Philadelphia",
            "county": "Philadelphia",
            "customers_affected": philly_total,
            "customers_served": philly_served,
            "percent_affected": round(percent, 2),
            "outages": philly_outages,
            "etr": None,
        })

    # Sort
    counties.sort(key=lambda x: -x["customers_affected"])
    municipalities.sort(key=lambda x: -x["customers_affected"])

    totals_item = totals.get("summaryFileData", {}).get("totals", [{}])[0]

    output = {
        "name": "power_outages_peco",
        "fetched_at": iso_utc_now(),
        "utility": {
            "name": "PECO",
            "customers_affected": to_int(totals_item.get("total_cust_a")),
            "customers_served": to_int(totals_item.get("total_cust_s")),
            "outages": to_int(totals_item.get("total_outages")),
            "percent_affected": totals_item.get("total_percent_cust_a", {}).get("val"),
        },
        "counties": counties,
        "municipalities": municipalities,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))

    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
