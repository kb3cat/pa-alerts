#!/usr/bin/env python3

import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

URL = "https://poweroutage.us/area/state/pennsylvania"
OUTPUT_FILE = "data/power_outages_pa.json"
PREVIOUS_FILE = "data/previous_power_outages_pa.json"
LOCAL_TZ = ZoneInfo("America/New_York")

HEADERS = {
    "User-Agent": "PennAlerts Power Board (kb3cat.github.io; contact via site)"
}

COUNTY_THRESHOLD_PERCENT = 1.0
COUNTY_THRESHOLD_OUTAGES = 50

PEMA_AREAS = {
    "Western Area": {
        "Beaver", "Butler", "Cambria", "Cameron", "Clarion", "Clearfield",
        "Crawford", "Elk", "Erie", "Forest", "Greene", "Indiana",
        "Jefferson", "Lawrence", "McKean", "Mercer", "Potter", "Venango",
        "Warren", "Washington", "Westmoreland", "Allegheny", "Armstrong",
        "Bedford", "Blair", "Fayette", "Somerset"
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


def clean_int(value: str) -> int:
    return int(value.replace(",", "").strip())


def clean_float(value: str) -> float:
    return float(value.replace("%", "").strip())


def normalize_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return text


def extract_statewide(text: str) -> dict:
    out_match = re.search(r"Customers Out\s+([\d,]+)", text)
    tracked_match = re.search(r"Customers Tracked\s+([\d,]+)", text)
    updated_match = re.search(r"# Pennsylvania Power Outages\s+Updated\s+(.+?)\s+Customers Out", text)

    if not out_match or not tracked_match:
        raise ValueError("Could not parse statewide totals from page")

    customers_out = clean_int(out_match.group(1))
    customers_tracked = clean_int(tracked_match.group(1))
    percent_out = round((customers_out / customers_tracked) * 100, 2) if customers_tracked else 0.0

    return {
        "customers_out": customers_out,
        "customers_tracked": customers_tracked,
        "percent_out": percent_out,
        "updated_text": updated_match.group(1).strip() if updated_match else None,
    }


def extract_section_block(text: str, start_marker: str, end_marker: str) -> str:
    start = text.find(start_marker)
    if start == -1:
        return ""

    end = text.find(end_marker, start)
    if end == -1:
        end = len(text)

    return text[start:end]


def extract_counties(text: str) -> list[dict]:
    block = extract_section_block(
        text,
        "## Outages by County",
        "## Pennsylvania Outage Summary"
    )

    pattern = re.compile(
        r"([A-Za-z .'-]+?)\s+Updated\s+(.+?)\s+([\d,]+)\s+Customers Out\s+([\d,]+)\s+Customers Tracked\s+([\d.]+)%\s+Outage Percent"
    )

    counties = []
    seen = set()

    for match in pattern.finditer(block):
        county = match.group(1).strip()
        if county in seen:
            continue
        seen.add(county)

        updated = match.group(2).strip()
        customers_out = clean_int(match.group(3))
        customers_tracked = clean_int(match.group(4))
        percent_out = clean_float(match.group(5))

        counties.append({
            "county": county,
            "updated_text": updated,
            "customers_out": customers_out,
            "customers_tracked": customers_tracked,
            "percent_out": percent_out,
        })

    return counties


def extract_utilities(text: str) -> list[dict]:
    block = extract_section_block(
        text,
        "## Outages by Utility",
        "## Outages by County"
    )

    pattern = re.compile(
        r"([A-Za-z0-9&/ .,'()-]+?)\s+Updated\s+(.+?)\s+([\d,]+)\s+Customers Out\s+([\d,]+)\s+Customers Tracked\s+([\d.]+)%\s+Outage Percent"
    )

    utilities = []
    seen = set()

    for match in pattern.finditer(block):
        utility = match.group(1).strip()
        if utility in seen:
            continue
        seen.add(utility)

        updated = match.group(2).strip()
        customers_out = clean_int(match.group(3))
        customers_tracked = clean_int(match.group(4))
        percent_out = clean_float(match.group(5))

        utilities.append({
            "utility": utility,
            "updated_text": updated,
            "customers_out": customers_out,
            "customers_tracked": customers_tracked,
            "percent_out": percent_out,
        })

    utilities.sort(key=lambda x: (-x["customers_out"], x["utility"]))
    return utilities


def build_significant_counties(counties: list[dict]) -> list[dict]:
    filtered = [
        c for c in counties
        if c["percent_out"] >= COUNTY_THRESHOLD_PERCENT and c["customers_out"] >= COUNTY_THRESHOLD_OUTAGES
    ]
    filtered.sort(key=lambda x: (-x["percent_out"], -x["customers_out"], x["county"]))
    return filtered


def build_ticker_items(counties: list[dict]) -> list[str]:
    return [
        f'{c["county"]}: {c["percent_out"]:.1f}% ({c["customers_out"]:,})'
        for c in counties
    ]


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


def load_previous_total() -> int | None:
    try:
        with open(PREVIOUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("statewide", {}).get("customers_out")
    except FileNotFoundError:
        return None
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


def write_previous_snapshot(payload: dict) -> None:
    previous_payload = {
        "saved_at": payload["generated_at"],
        "statewide": payload["statewide"],
    }
    with open(PREVIOUS_FILE, "w", encoding="utf-8") as f:
        json.dump(previous_payload, f, indent=2)


def main():
    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    text = normalize_text(response.text)

    statewide = extract_statewide(text)
    counties = extract_counties(text)
    utilities = extract_utilities(text)
    significant_counties = build_significant_counties(counties)
    pema_totals = build_pema_totals(counties)

    previous_total = load_previous_total()
    trend = build_trend(statewide["customers_out"], previous_total)

    payload = {
        "generated_at": datetime.now(LOCAL_TZ).isoformat(),
        "source": {
            "name": "PowerOutage.us",
            "url": URL,
        },
        "thresholds": {
            "county_percent_min": COUNTY_THRESHOLD_PERCENT,
            "county_outages_min": COUNTY_THRESHOLD_OUTAGES,
        },
        "statewide": statewide,
        "trend_since_last_update": trend,
        "pema_areas": pema_totals,
        "counties": counties,
        "significant_counties": significant_counties,
        "utilities": utilities,
        "ticker_items": build_ticker_items(significant_counties),
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    write_previous_snapshot(payload)

    print(f"Wrote {OUTPUT_FILE}")
    print(f'Statewide out: {statewide["customers_out"]:,}')
    print(f'Counties parsed: {len(counties)}')
    print(f'Utilities parsed: {len(utilities)}')
    print(f'Significant counties: {len(significant_counties)}')
    print(f'Trend: {trend["display"]}')


if __name__ == "__main__":
    main()
