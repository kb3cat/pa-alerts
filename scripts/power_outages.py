#!/usr/bin/env python3

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

OUTPUT_FILE = "data/power_outages_pa.json"
PREVIOUS_FILE = "data/previous_power_outages_pa.json"
LOCAL_TZ = ZoneInfo("America/New_York")

FIRSTENERGY_URL = "https://www.firstenergycorp.com/my-town-search.areaSearch.json"

COUNTY_THRESHOLD_PERCENT = 1.0
COUNTY_THRESHOLD_OUTAGES = 50


def parse_percent(value) -> float:
    if value is None:
        return 0.0

    s = str(value).strip().replace("%", "")
    if s in {"", "-", "--"}:
        return 0.0
    if s.startswith("<"):
        return 0.0

    try:
        return float(s)
    except ValueError:
        return 0.0


def fetch_firstenergy_data() -> dict:
    session = requests.Session()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/134.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.firstenergycorp.com",
        "Referer": "https://www.firstenergycorp.com/outages_help/current_outages_maps/my-town-search.html",
        "X-Requested-With": "XMLHttpRequest",
    }

    response = session.post(
        FIRSTENERGY_URL,
        headers=headers,
        data={"stateAbbreviation": "pa"},
        timeout=30,
        allow_redirects=True,
    )

    print(f"HTTP status: {response.status_code}")
    print(f"Content-Type: {response.headers.get('Content-Type')}")
    print(f"Final URL: {response.url}")
    print("Response preview:")
    print(response.text[:1000])

    response.raise_for_status()

    if not response.text.strip():
        raise RuntimeError("FirstEnergy returned empty response")

    try:
        return response.json()
    except Exception as e:
        raise RuntimeError(
            f"FirstEnergy did not return JSON. "
            f"Status={response.status_code}, "
            f"Content-Type={response.headers.get('Content-Type')}, "
            f"Preview={response.text[:300]!r}"
        ) from e


def parse_firstenergy(data: dict):
    counties = []
    municipalities = []

    map_counties = data.get("mapCounties", {})

    for county_key, county_obj in map_counties.items():
        county_name = county_obj.get("name", county_key.title())
        area = county_obj.get("areaData", {})

        county_row = {
            "county": county_name,
            "customers_out": int(area.get("customerOutages", 0) or 0),
            "customers_tracked": int(area.get("totalCustomers", 0) or 0),
            "percent_out": parse_percent(area.get("percentOut")),
            "etr": area.get("estimatedTimeRestored") or "",
            "source": "FirstEnergy",
        }

        counties.append(county_row)

        towns = county_obj.get("mapTowns", {})
        for _, town_obj in towns.items():
            t_area = town_obj.get("areaData", {})
            municipalities.append({
                "county": county_name,
                "municipality": town_obj.get("name"),
                "customers_out": int(t_area.get("customerOutages", 0) or 0),
                "customers_tracked": int(t_area.get("totalCustomers", 0) or 0),
                "percent_out": parse_percent(t_area.get("percentOut")),
                "etr": t_area.get("estimatedTimeRestored") or "",
                "source": "FirstEnergy",
            })

    counties.sort(key=lambda x: x["county"])
    municipalities.sort(key=lambda x: (x["county"], x["municipality"]))

    return counties, municipalities


def load_previous_total():
    try:
        with open(PREVIOUS_FILE, "r") as f:
            return json.load(f).get("statewide", {}).get("customers_out")
    except:
        return None


def build_trend(current, previous):
    if previous is None:
        return {"direction": "flat", "delta": 0, "display": "No Change"}

    delta = current - previous

    if delta > 0:
        return {"direction": "up", "delta": delta, "display": f"▲ +{delta:,}"}
    if delta < 0:
        return {"direction": "down", "delta": delta, "display": f"▼ {delta:,}"}

    return {"direction": "flat", "delta": 0, "display": "No Change"}


def build_significant(counties):
    rows = [
        c for c in counties
        if c["percent_out"] >= COUNTY_THRESHOLD_PERCENT
        and c["customers_out"] >= COUNTY_THRESHOLD_OUTAGES
    ]
    return sorted(rows, key=lambda x: (-x["percent_out"], -x["customers_out"]))


def build_ticker(counties):
    return [
        f'{c["county"]}: {c["percent_out"]:.1f}% ({c["customers_out"]:,})'
        for c in counties
    ]


def main():
    raw = fetch_firstenergy_data()
    counties, municipalities = parse_firstenergy(raw)

    total_out = sum(c["customers_out"] for c in counties)
    total_tracked = sum(c["customers_tracked"] for c in counties)

    statewide = {
        "customers_out": total_out,
        "customers_tracked": total_tracked,
        "percent_out": round((total_out / total_tracked) * 100, 2) if total_tracked else 0.0,
        "updated_text": datetime.now(LOCAL_TZ).strftime("%m/%d/%y %I:%M %p"),
    }

    prev = load_previous_total()
    trend = build_trend(total_out, prev)
    significant = build_significant(counties)

    payload = {
        "generated_at": datetime.now(LOCAL_TZ).isoformat(),
        "statewide": statewide,
        "trend_since_last_update": trend,
        "utilities": [{
            "utility": "FirstEnergy",
            "customers_out": total_out,
            "customers_tracked": total_tracked,
            "percent_out": statewide["percent_out"],
        }],
        "counties": counties,
        "significant_counties": significant,
        "ticker_items": build_ticker(significant),
        "municipalities": municipalities,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(payload, f, indent=2)

    with open(PREVIOUS_FILE, "w") as f:
        json.dump({"statewide": statewide}, f, indent=2)

    print("SUCCESS")
    print(f"Counties: {len(counties)}")
    print(f"Municipalities: {len(municipalities)}")
    print(f"Statewide Out: {total_out:,}")
    print(f"Trend: {trend['display']}")


if __name__ == "__main__":
    main()
