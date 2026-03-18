#!/usr/bin/env python3

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from playwright.sync_api import sync_playwright

OUTPUT_FILE = "data/power_outages_pa.json"
PREVIOUS_FILE = "data/previous_power_outages_pa.json"
LOCAL_TZ = ZoneInfo("America/New_York")

FIRSTENERGY_PAGE_URL = (
    "https://www.firstenergycorp.com/outages_help/current_outages_maps/"
    "my-town-search.html"
)
FIRSTENERGY_DATA_URL = (
    "https://www.firstenergycorp.com/content/customer/outages_help/"
    "current_outages_maps/my-town-search.areaSearch.json"
)

COUNTY_THRESHOLD_PERCENT = 1.0
COUNTY_THRESHOLD_OUTAGES = 50

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


def load_previous_total():
    try:
        with open(PREVIOUS_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("statewide", {}).get("customers_out")
    except Exception:
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
    return sorted(rows, key=lambda x: (-x["percent_out"], -x["customers_out"], x["county"]))


def build_ticker(counties):
    return [
        f'{c["county"]}: {c["percent_out"]:.1f}% ({c["customers_out"]:,})'
        for c in counties
    ]


def build_pema_totals(counties):
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


def fetch_firstenergy_data() -> dict:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:148.0) "
                "Gecko/20100101 Firefox/148.0"
            ),
            locale="en-US",
        )
        page = context.new_page()

        print("Loading FirstEnergy My Town page...")
        page.goto(FIRSTENERGY_PAGE_URL, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(3000)

        print("Posting to FirstEnergy outage endpoint from browser context...")
        api_response = context.request.post(
            FIRSTENERGY_DATA_URL,
            headers={
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://www.firstenergycorp.com",
                "Referer": FIRSTENERGY_PAGE_URL,
                "X-Requested-With": "XMLHttpRequest",
            },
            form={"stateAbbreviation": "pa"},
            timeout=60000,
        )

        print(f"HTTP status: {api_response.status}")
        print(f"Content-Type: {api_response.headers.get('content-type')}")

        text = api_response.text()
        print("Response preview:")
        print(text[:500])

        if api_response.status != 200:
            raise RuntimeError(f"FirstEnergy returned HTTP {api_response.status}")

        if not text.strip():
            raise RuntimeError("FirstEnergy returned empty response")

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"FirstEnergy did not return JSON. "
                f"Content-Type={api_response.headers.get('content-type')}, "
                f"Preview={text[:300]!r}"
            ) from e
        finally:
            browser.close()

        return data


def parse_firstenergy(data: dict):
    counties = []
    municipalities = []

    map_counties = data.get("mapCounties", {})

    for county_key, county_obj in map_counties.items():
        county_name = county_obj.get("name", county_key.title())
        area = county_obj.get("areaData", {})

        counties.append({
            "county": county_name,
            "customers_out": int(area.get("customerOutages", 0) or 0),
            "customers_tracked": int(area.get("totalCustomers", 0) or 0),
            "percent_out": parse_percent(area.get("percentOut")),
            "etr": area.get("estimatedTimeRestored") or "",
            "source": "FirstEnergy",
        })

        for _, town_obj in county_obj.get("mapTowns", {}).items():
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
    municipalities.sort(key=lambda x: (x["county"], x["municipality"] or ""))

    return counties, municipalities


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
    pema_totals = build_pema_totals(counties)

    payload = {
        "generated_at": datetime.now(LOCAL_TZ).isoformat(),
        "source_strategy": "FirstEnergy via Playwright browser session",
        "statewide": statewide,
        "trend_since_last_update": trend,
        "pema_areas": pema_totals,
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

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    with open(PREVIOUS_FILE, "w", encoding="utf-8") as f:
        json.dump({"statewide": statewide}, f, indent=2)

    print("SUCCESS")
    print(f"Counties: {len(counties)}")
    print(f"Municipalities: {len(municipalities)}")
    print(f"Statewide Out: {total_out:,}")
    print(f"Trend: {trend['display']}")


if __name__ == "__main__":
    main()
