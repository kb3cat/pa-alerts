#!/usr/bin/env python3

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

OUTPUT_FILE = "data/power_outages_pa.json"
PREVIOUS_FILE = "data/previous_power_outages_pa.json"
LOCAL_TZ = ZoneInfo("America/New_York")

COUNTY_THRESHOLD_PERCENT = 1.0
COUNTY_THRESHOLD_OUTAGES = 50

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://www.firstenergycorp.com",
    "Referer": "https://www.firstenergycorp.com/",
}

FIRSTENERGY_URL = "https://www.firstenergycorp.com/my-town-search.areaSearch.json"

PEMA_AREAS = {
    "Western Area": {
        "Allegheny", "Armstrong", "Beaver", "Bedford", "Blair", "Butler",
        "Cambria", "Cameron", "Clarion", "Clearfield", "Crawford", "Elk",
        "Erie", "Fayette", "Forest", "Greene", "Indiana", "Jefferson",
        "Lawrence", "McKean", "Mercer", "Potter", "Somerset", "Venango",
        "Warren", "Washington", "Westmoreland"
    },
    "Central Area": {
        "Adams", "Berks", "Bradford", "Centre", "Clinton", "Columbia",
        "Cumberland", "Dauphin", "Franklin", "Fulton", "Huntingdon",
        "Juniata", "Lackawanna", "Lancaster", "Lebanon", "Luzerne",
        "Lycoming", "Mifflin", "Montour", "Northumberland", "Perry",
        "Schuylkill", "Snyder", "Sullivan", "Susquehanna", "Tioga",
        "Union", "Wyoming", "York"
    },
    "Eastern Area": {
        "Bucks", "Carbon", "Chester", "Delaware", "Lehigh", "Monroe",
        "Montgomery", "Northampton", "Philadelphia", "Pike", "Wayne"
    },
}


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


def load_previous_total() -> int | None:
    try:
        with open(PREVIOUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("statewide", {}).get("customers_out")
    except Exception:
        return None


def build_trend(current_total: int, previous_total: int | None) -> dict:
    if previous_total is None:
        return {
            "direction": "flat",
            "delta": 0,
            "display": "No Change",
        }

    delta = current_total - previous_total

    if delta > 0:
        return {
            "direction": "up",
            "delta": delta,
            "display": f"▲ +{delta:,}",
        }
    if delta < 0:
        return {
            "direction": "down",
            "delta": delta,
            "display": f"▼ {delta:,}",
        }

    return {
        "direction": "flat",
        "delta": 0,
        "display": "No Change",
    }


def build_pema_totals(counties: list[dict]) -> dict:
    totals = {
        "Western Area": 0,
        "Central Area": 0,
        "Eastern Area": 0,
        "Unmapped": 0,
    }

    for county in counties:
        county_name = county["county"]
        matched = False

        for area_name, area_counties in PEMA_AREAS.items():
            if county_name in area_counties:
                totals[area_name] += county["customers_out"]
                matched = True
                break

        if not matched:
            totals["Unmapped"] += county["customers_out"]

    return totals


def build_significant_counties(counties: list[dict]) -> list[dict]:
    rows = [
        c for c in counties
        if c["percent_out"] >= COUNTY_THRESHOLD_PERCENT
        and c["customers_out"] >= COUNTY_THRESHOLD_OUTAGES
    ]
    rows.sort(key=lambda x: (-x["percent_out"], -x["customers_out"], x["county"]))
    return rows


def build_ticker_items(counties: list[dict]) -> list[str]:
    return [
        f'{c["county"]}: {c["percent_out"]:.1f}% ({c["customers_out"]:,})'
        for c in counties
    ]


def write_previous_snapshot(payload: dict) -> None:
    previous_payload = {
        "saved_at": payload["generated_at"],
        "statewide": payload["statewide"],
    }
    with open(PREVIOUS_FILE, "w", encoding="utf-8") as f:
        json.dump(previous_payload, f, indent=2)


def fetch_firstenergy_data() -> dict:
    session = requests.Session()
    response = session.post(
        FIRSTENERGY_URL,
        headers=HEADERS,
        data={"stateAbbreviation": "pa"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def parse_firstenergy(payload: dict) -> tuple[list[dict], list[dict]]:
    counties_out = []
    municipalities_out = []

    map_counties = payload.get("mapCounties", {})

    for county_key, county_obj in map_counties.items():
        county_name = county_obj.get("name", county_key.title())
        area_data = county_obj.get("areaData", {})

        county_row = {
            "county": county_name,
            "customers_out": int(area_data.get("customerOutages", 0) or 0),
            "customers_tracked": int(area_data.get("totalCustomers", 0) or 0),
            "percent_out": parse_percent(area_data.get("percentOut")),
            "etr": area_data.get("estimatedTimeRestored") or "",
            "source": "FirstEnergy",
        }
        counties_out.append(county_row)

        map_towns = county_obj.get("mapTowns", {})
        for town_key, town_obj in map_towns.items():
            town_name = town_obj.get("name", town_key.title())
            town_data = town_obj.get("areaData", {})

            municipalities_out.append({
                "county": county_name,
                "municipality": town_name,
                "customers_out": int(town_data.get("customerOutages", 0) or 0),
                "customers_tracked": int(town_data.get("totalCustomers", 0) or 0),
                "percent_out": parse_percent(town_data.get("percentOut")),
                "etr": town_data.get("estimatedTimeRestored") or "",
                "source": "FirstEnergy",
            })

    counties_out.sort(key=lambda x: x["county"])
    municipalities_out.sort(key=lambda x: (x["county"], x["municipality"]))
    return counties_out, municipalities_out


def main():
    raw = fetch_firstenergy_data()
    counties, municipalities = parse_firstenergy(raw)

    statewide_out = sum(c["customers_out"] for c in counties)
    statewide_tracked = sum(c["customers_tracked"] for c in counties)

    statewide = {
        "customers_out": statewide_out,
        "customers_tracked": statewide_tracked,
        "percent_out": round((statewide_out / statewide_tracked) * 100, 2) if statewide_tracked else 0.0,
        "updated_text": datetime.now(LOCAL_TZ).strftime("%m/%d/%y %I:%M %p %Z"),
    }

    previous_total = load_previous_total()
    trend = build_trend(statewide_out, previous_total)
    significant_counties = build_significant_counties(counties)
    pema_totals = build_pema_totals(counties)

    utilities = [{
        "utility": "FirstEnergy",
        "customers_out": statewide_out,
        "customers_tracked": statewide_tracked,
        "percent_out": statewide["percent_out"],
        "updated_text": statewide["updated_text"],
    }]

    payload = {
        "generated_at": datetime.now(LOCAL_TZ).isoformat(),
        "source_strategy": "FirstEnergy only",
        "sources": [
            {
                "name": "FirstEnergy",
                "url": FIRSTENERGY_URL,
                "method": "POST",
            }
        ],
        "thresholds": {
            "county_percent_min": COUNTY_THRESHOLD_PERCENT,
            "county_outages_min": COUNTY_THRESHOLD_OUTAGES,
        },
        "statewide": statewide,
        "trend_since_last_update": trend,
        "pema_areas": pema_totals,
        "utilities": utilities,
        "counties": counties,
        "significant_counties": significant_counties,
        "ticker_items": build_ticker_items(significant_counties),
        "municipalities": municipalities,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    write_previous_snapshot(payload)

    print(f"Wrote {OUTPUT_FILE}")
    print(f"FirstEnergy counties parsed: {len(counties)}")
    print(f"FirstEnergy municipalities parsed: {len(municipalities)}")
    print(f'Statewide out: {statewide_out:,}')
    print(f'Trend: {trend["display"]}')


if __name__ == "__main__":
    main()
