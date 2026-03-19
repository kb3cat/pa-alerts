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
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return 0


def has_mask(v):
    return isinstance(v, dict) and "mask" in v


def calc_percent(out_count, served_count):
    if served_count > 0:
        return round((out_count / served_count) * 100, 2)
    return 0.0


def percent_display(percent_value, masked=False):
    if masked:
        return "<5%"
    return f"{percent_value:.2f}%"


def count_display(count_value, masked=False):
    if masked:
        return "<5"
    return str(int(count_value or 0))


def is_philly_zip(name):
    return str(name).isdigit() and str(name).startswith("19") and len(str(name)) == 5


def build_county_row(county):
    county_name = county.get("name", "").title()
    county_masked = has_mask(county.get("cust_a")) or has_mask(county.get("percent_cust_a"))

    # For masked county rows, do not trust the raw customer count.
    county_out = 0 if county_masked else to_int(county.get("cust_a"))
    county_served = to_int(county.get("cust_s"))
    county_outages = to_int(county.get("n_out"))
    county_percent = calc_percent(county_out, county_served)

    return {
        "name": county_name,
        "customers_affected": county_out,
        "customers_affected_display": count_display(county_out, county_masked),
        "customers_served": county_served,
        "percent_affected": county_percent,
        "percent_affected_display": percent_display(county_percent, county_masked),
        "outages": county_outages,
        "etr": county.get("etr"),
        "masked": county_masked,
    }


def build_muni_row(muni, county_name):
    name_raw = muni.get("name", "")
    name = name_raw.title()

    masked = has_mask(muni.get("cust_a")) or has_mask(muni.get("percent_cust_a"))

    # For masked muni rows, do not trust the raw customer count.
    cust = 0 if masked else to_int(muni.get("cust_a"))
    served = to_int(muni.get("cust_s"))
    outages = to_int(muni.get("n_out"))
    percent = calc_percent(cust, served)

    return {
        "name": name,
        "county": county_name,
        "customers_affected": cust,
        "customers_affected_display": count_display(cust, masked),
        "customers_served": served,
        "percent_affected": percent,
        "percent_affected_display": percent_display(percent, masked),
        "outages": outages,
        "etr": muni.get("etr"),
        "masked": masked,
        "_raw_name": name_raw,
    }


def main():
    totals = fetch_json(TOTALS_URL)
    report = fetch_json(REPORT_URL)

    file_data = report.get("file_data", {})

    counties = []
    municipalities = []

    philly_total = 0
    philly_served = 0
    philly_outages = 0
    philly_masked = False

    for county in file_data.get("areas", []):
        county_row = build_county_row(county)
        county_name = county_row["name"]
        counties.append(county_row)

        for muni in county.get("areas", []):
            muni_row = build_muni_row(muni, county_name)
            name_raw = muni_row.pop("_raw_name", "")

            # Aggregate Philadelphia ZIPs into one Philadelphia row
            if county_name == "Philadelphia" and is_philly_zip(name_raw):
                philly_served += muni_row["customers_served"]
                philly_outages += muni_row["outages"]

                if muni_row["masked"]:
                    philly_masked = True
                else:
                    philly_total += muni_row["customers_affected"]

                continue

            municipalities.append(muni_row)

    # Add aggregated Philadelphia row
    if philly_served > 0 or philly_outages > 0 or philly_total > 0:
        philly_percent = calc_percent(philly_total, philly_served)

        municipalities.append({
            "name": "Philadelphia",
            "county": "Philadelphia",
            "customers_affected": philly_total,
            "customers_affected_display": count_display(philly_total, philly_masked),
            "customers_served": philly_served,
            "percent_affected": philly_percent,
            "percent_affected_display": percent_display(philly_percent, philly_masked),
            "outages": philly_outages,
            "etr": None,
            "masked": philly_masked,
        })

    counties.sort(key=lambda x: (-x["customers_affected"], x["name"]))
    municipalities.sort(key=lambda x: (-x["customers_affected"], x["name"]))

    totals_item = totals.get("summaryFileData", {}).get("totals", [{}])[0]
    total_out = to_int(totals_item.get("total_cust_a"))
    total_served = to_int(totals_item.get("total_cust_s"))
    total_percent = calc_percent(total_out, total_served)

    output = {
        "name": "power_outages_peco",
        "fetched_at": iso_utc_now(),
        "utility": {
            "name": "PECO",
            "customers_affected": total_out,
            "customers_affected_display": str(total_out),
            "customers_served": total_served,
            "outages": to_int(totals_item.get("total_outages")),
            "percent_affected": total_percent,
            "percent_affected_display": f"{total_percent:.2f}%",
        },
        "counties": counties,
        "municipalities": municipalities,
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")

    print(f"Wrote {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
