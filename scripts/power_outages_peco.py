#!/usr/bin/env python3
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

TOTALS_URL = "https://kubra.io/data/e1548a52-76e1-405b-a27d-172feee110b9/public/summary-1/data.json"
REPORT_URL = "https://kubra.io/data/ee4132a9-7316-453a-b383-9e048a28a709/public/reports/a36a6292-1c55-44de-a6a9-44fedf9482ee_report.json"

OUTPUT_PATH = Path("peco_outages.json")
TIMEOUT = 30


def fetch_json(url: str) -> dict:
    resp = requests.get(
        url,
        timeout=TIMEOUT,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; PennAlerts/1.0; +https://kb3cat.github.io/)"
        },
    )
    resp.raise_for_status()
    return resp.json()


def num_val(obj, default=0):
    if isinstance(obj, dict):
        return obj.get("val", default)
    if isinstance(obj, (int, float)):
        return obj
    return default


def has_mask(obj) -> bool:
    return isinstance(obj, dict) and "mask" in obj


def count_display(obj) -> str:
    if isinstance(obj, dict) and "mask" in obj:
        return f"Less than {obj['mask']}"
    if isinstance(obj, dict):
        return str(obj.get("val", 0))
    return str(obj if obj is not None else 0)


def percent_display(obj) -> str:
    if isinstance(obj, dict) and "mask" in obj:
        return f"Less than {int(obj['mask'])}%"
    if isinstance(obj, dict):
        val = obj.get("val", 0)
    else:
        val = obj if obj is not None else 0
    return f"{val:.2f}%"


def pretty_name(name: str) -> str:
    if not name:
        return ""
    return name.title() if name.isupper() else name


def parse_totals(totals_data: dict, report_data: dict) -> dict:
    summary = totals_data.get("summaryFileData", {})
    totals_list = summary.get("totals", [])
    totals_item = totals_list[0] if totals_list else {}

    report_totals = report_data.get("file_data", {}).get("totals", {})

    affected_obj = totals_item.get("total_cust_a", report_totals.get("cust_a", {"val": 0}))
    served = totals_item.get("total_cust_s", report_totals.get("cust_s", 0))
    outages = totals_item.get("total_outages", report_totals.get("n_out", 0))
    percent_obj = totals_item.get("total_percent_cust_a", report_totals.get("percent_cust_a", {"val": 0}))
    generated = summary.get("date_generated")

    return {
        "customers_affected": num_val(affected_obj),
        "customers_affected_display": count_display(affected_obj),
        "customers_served": served,
        "outages": outages,
        "percent_affected": num_val(percent_obj),
        "percent_affected_display": percent_display(percent_obj),
        "date_generated": generated,
    }


def parse_report(report_data: dict) -> tuple[list, list]:
    areas = report_data.get("file_data", {}).get("areas", [])

    counties = []
    municipalities = []

    for county in areas:
        county_name_raw = (county.get("name") or "").strip()
        county_name = pretty_name(county_name_raw)

        county_cust_a = county.get("cust_a", {"val": 0})
        county_percent = county.get("percent_cust_a", {"val": 0})

        counties.append(
            {
                "name": county_name,
                "raw_name": county_name_raw,
                "customers_affected": num_val(county_cust_a),
                "customers_affected_display": count_display(county_cust_a),
                "customers_served": county.get("cust_s", 0),
                "percent_affected": num_val(county_percent),
                "percent_affected_display": percent_display(county_percent),
                "outages": county.get("n_out", 0),
                "etr": county.get("etr"),
                "masked": has_mask(county_cust_a),
            }
        )

        for muni in county.get("areas", []):
            muni_name_raw = (muni.get("name") or "").strip()
            muni_name = pretty_name(muni_name_raw)

            muni_cust_a = muni.get("cust_a", {"val": 0})
            muni_percent = muni.get("percent_cust_a", {"val": 0})

            municipalities.append(
                {
                    "name": muni_name,
                    "raw_name": muni_name_raw,
                    "county": county_name,
                    "customers_affected": num_val(muni_cust_a),
                    "customers_affected_display": count_display(muni_cust_a),
                    "customers_served": muni.get("cust_s", 0),
                    "percent_affected": num_val(muni_percent),
                    "percent_affected_display": percent_display(muni_percent),
                    "outages": muni.get("n_out", 0),
                    "etr": muni.get("etr"),
                    "masked": has_mask(muni_cust_a),
                }
            )

    counties.sort(key=lambda x: (-x["customers_affected"], -x["outages"], x["name"]))
    municipalities.sort(key=lambda x: (-x["customers_affected"], -x["outages"], x["county"], x["name"]))

    return counties, municipalities


def main():
    try:
        totals_data = fetch_json(TOTALS_URL)
        report_data = fetch_json(REPORT_URL)

        totals = parse_totals(totals_data, report_data)
        counties, municipalities = parse_report(report_data)

        output = {
            "name": "peco_outages",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source_urls": {
                "totals": TOTALS_URL,
                "report": REPORT_URL,
            },
            "utility": {
                "name": "PECO",
                "customers_affected": totals["customers_affected"],
                "customers_affected_display": totals["customers_affected_display"],
                "customers_served": totals["customers_served"],
                "outages": totals["outages"],
                "percent_affected": totals["percent_affected"],
                "percent_affected_display": totals["percent_affected_display"],
                "date_generated": totals["date_generated"],
            },
            "top_counties": counties[:5],
            "top_municipalities": [m for m in municipalities if m["customers_affected"] > 0][:5],
            "counties": counties,
            "municipalities": municipalities,
        }

        OUTPUT_PATH.write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(f"Wrote {OUTPUT_PATH}")

    except requests.HTTPError as e:
        print(f"HTTP error: {e}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as e:
        print(f"Request error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
