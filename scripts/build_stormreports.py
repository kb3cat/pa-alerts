#!/usr/bin/env python3

import json
import os
import re
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

API_BASE = "https://api.weather.gov"
OUTPUT_FILE = "data/stormreports.json"
LOCAL_TZ = ZoneInfo("America/New_York")

OFFICES = {
    "PBZ": "Pittsburgh",
    "CLE": "Cleveland",
    "CTP": "State College",
    "BGM": "Binghamton",
    "PHI": "Mount Holly",
}

HEADERS = {
    "User-Agent": "PennsylvaniaStormReports/1.0 (GitHub Actions)",
    "Accept": "application/geo+json, application/ld+json, application/json",
}

PRODUCT_LIMIT_PER_OFFICE = 25

ROW1_RE = re.compile(
    r"^(?P<time>\d{3,4}\s[AP]M)\s+"
    r"(?P<event>.{1,22}?)\s{2,}"
    r"(?P<location>.{1,24}?)\s+"
    r"(?P<lat>\d{2}\.\d{2}[NS])\s+"
    r"(?P<lon>\d{2,3}\.\d{2}[EW])\s*$"
)


def clean_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def latlon_to_float(value: str) -> float:
    value = value.strip()
    hemi = value[-1]
    num = float(value[:-1])
    if hemi in ("S", "W"):
        num *= -1
    return round(num, 4)


def parse_lsr_datetime(time_str: str, date_str: str) -> datetime:
    month, day, year = map(int, date_str.split("/"))
    t, ampm = time_str.split()

    if len(t) == 3:
        hour = int(t[0])
        minute = int(t[1:])
    else:
        hour = int(t[:2])
        minute = int(t[2:])

    if ampm == "AM":
        if hour == 12:
            hour = 0
    else:
        if hour != 12:
            hour += 12

    return datetime(year, month, day, hour, minute, tzinfo=LOCAL_TZ)


def parse_api_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        value = value.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LOCAL_TZ)
        return dt.astimezone(LOCAL_TZ)
    except Exception:
        return None


def parse_row2(line: str) -> tuple[str, str, str, str, str] | None:
    """
    Parse the second LSR row by splitting on 2+ spaces.

    Typical examples:
    03/08/2026  1.00 INCH  BEAVER            PA  TRAINED SPOTTER
    03/08/2026             BUTLER            PA  PUBLIC
    03/08/2026  58 MPH     CRAWFORD          PA  911 CALL CENTER
    """
    parts = re.split(r"\s{2,}", line.strip())

    if len(parts) < 4:
        return None

    date_str = parts[0].strip()
    state = parts[-2].strip()
    source = parts[-1].strip()
    middle = parts[1:-2]

    if not middle:
        return None

    if len(middle) == 1:
        magnitude = ""
        county = middle[0].strip()
    else:
        magnitude = middle[0].strip()
        county = " ".join(p.strip() for p in middle[1:]).strip()

    return date_str, magnitude, county, state, source


def fetch_json(url: str) -> dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def get_product_ids_for_office(office: str) -> list[str]:
    url = f"{API_BASE}/products/types/LSR/locations/{office}"
    payload = fetch_json(url)

    ids = []
    graph = payload.get("@graph", [])
    for item in graph:
        product_id = item.get("id") or item.get("@id")
        if not product_id:
            continue
        if product_id.startswith("http"):
            product_id = product_id.rstrip("/").split("/")[-1]
        ids.append(product_id)

    seen = set()
    unique = []
    for pid in ids:
        if pid in seen:
            continue
        seen.add(pid)
        unique.append(pid)
        if len(unique) >= PRODUCT_LIMIT_PER_OFFICE:
            break

    return unique


def fetch_product_payload(product_id: str) -> dict:
    url = f"{API_BASE}/products/{product_id}"
    return fetch_json(url)


def extract_product_text(payload: dict) -> str:
    return payload.get("productText", "") or payload.get("text", "") or ""


def extract_product_issued_time(payload: dict) -> datetime | None:
    candidate_fields = [
        "issuanceTime",
        "issueTime",
        "effectiveTime",
        "sent",
        "created",
        "updateTime",
        "date",
    ]

    for field in candidate_fields:
        dt = parse_api_datetime(payload.get(field))
        if dt is not None:
            return dt

    graph = payload.get("@graph")
    if isinstance(graph, list):
        for item in graph:
            if not isinstance(item, dict):
                continue
            for field in candidate_fields:
                dt = parse_api_datetime(item.get(field))
                if dt is not None:
                    return dt

    return None


def build_report_id(
    office: str,
    issued_dt: datetime,
    report_dt: datetime,
    location: str,
    county: str,
    lat: float,
    lon: float,
) -> str:
    safe_loc = re.sub(r"[^A-Za-z0-9]+", "-", location.strip().lower()).strip("-")
    safe_county = re.sub(r"[^A-Za-z0-9]+", "-", county.strip().lower()).strip("-")
    safe_loc = safe_loc[:24] if safe_loc else "loc"
    safe_county = safe_county[:24] if safe_county else "county"
    lat_part = str(lat).replace("-", "m").replace(".", "")
    lon_part = str(lon).replace("-", "m").replace(".", "")
    return (
        f"{office}-"
        f"{issued_dt.strftime('%Y%m%d-%H%M')}-"
        f"{report_dt.strftime('%Y%m%d-%H%M')}-"
        f"{safe_county}-{safe_loc}-{lat_part}-{lon_part}"
    )


def parse_lsr_text(text: str, office: str, issued_dt: datetime) -> list[dict]:
    lines = text.splitlines()
    results = []
    idx = 0

    while idx < len(lines) - 1:
        line1 = lines[idx].rstrip()
        line2 = lines[idx + 1].rstrip()

        m1 = ROW1_RE.match(line1)
        row2 = parse_row2(line2)

        if not m1 or not row2:
            idx += 1
            continue

        time_str = clean_spaces(m1.group("time"))
        event = clean_spaces(m1.group("event"))
        location = clean_spaces(m1.group("location"))
        lat = latlon_to_float(m1.group("lat"))
        lon = latlon_to_float(m1.group("lon"))

        date_str, magnitude, county, state, source = row2
        date_str = clean_spaces(date_str)
        magnitude = clean_spaces(magnitude)
        county = clean_spaces(county)
        state = clean_spaces(state)
        source = clean_spaces(source)

        # Pennsylvania-only board
        if state != "PA":
            idx += 2
            continue

        try:
            report_dt = parse_lsr_datetime(time_str, date_str)
        except Exception:
            idx += 2
            continue

        remarks_lines = []
        j = idx + 2
        while j < len(lines):
            next_line = lines[j].rstrip()

            if ROW1_RE.match(next_line):
                break
            if next_line.startswith("&&") or next_line.startswith("$$"):
                break
            if next_line.strip():
                remarks_lines.append(clean_spaces(next_line))
            j += 1

        remarks = clean_spaces(" ".join(remarks_lines))
        if remarks.startswith("..."):
            remarks = remarks.lstrip(". ").strip()

        results.append({
            "id": build_report_id(office, issued_dt, report_dt, location, county, lat, lon),
            "office": office,
            "office_name": OFFICES[office],
            "event": event,
            "magnitude": "" if not magnitude or magnitude.upper() == "UNK" else magnitude,
            "location": location,
            "county": county,
            "source": source,
            "remarks": remarks,
            "issued_time": issued_dt.isoformat(timespec="seconds"),
            "report_time": report_dt.isoformat(timespec="seconds"),
            "lat": lat,
            "lon": lon,
        })

        idx = j

    return results


def dedupe_reports(reports: list[dict]) -> list[dict]:
    seen = set()
    output = []

    for report in reports:
        key = (
            report["office"],
            report["event"],
            report["location"],
            report["county"],
            report.get("issued_time"),
            report["report_time"],
            report["lat"],
            report["lon"],
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(report)

    return output


def filter_last_24_hours_by_issued_time(reports: list[dict]) -> tuple[list[dict], datetime, datetime]:
    now_local = datetime.now(LOCAL_TZ)
    window_start = now_local - timedelta(hours=24)

    filtered = []
    for report in reports:
        try:
            issued_dt = datetime.fromisoformat(report["issued_time"])
        except Exception:
            continue
        if window_start <= issued_dt <= now_local:
            filtered.append(report)

    filtered.sort(key=lambda r: (r["issued_time"], r["report_time"]), reverse=True)
    return filtered, window_start, now_local


def load_previous_payload() -> dict:
    if not os.path.exists(OUTPUT_FILE):
        return {}

    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        print(f"Failed to read previous {OUTPUT_FILE}: {exc}")
        return {}


def mark_new_reports(reports: list[dict], previous_payload: dict) -> tuple[list[dict], int]:
    previous_reports = previous_payload.get("reports", []) if isinstance(previous_payload, dict) else []
    previous_ids = {
        report.get("id")
        for report in previous_reports
        if isinstance(report, dict) and report.get("id")
    }

    new_count = 0
    marked = []

    for report in reports:
        updated = dict(report)
        is_new = updated.get("id") not in previous_ids
        updated["is_new"] = is_new
        if is_new:
            new_count += 1
        marked.append(updated)

    return marked, new_count


def build_payload(previous_payload: dict | None = None) -> dict:
    if previous_payload is None:
        previous_payload = {}

    all_reports = []

    for office in OFFICES:
        try:
            product_ids = get_product_ids_for_office(office)
        except Exception as exc:
            print(f"[{office}] failed to fetch product list: {exc}")
            continue

        for product_id in product_ids:
            try:
                payload = fetch_product_payload(product_id)
                text = extract_product_text(payload)
                issued_dt = extract_product_issued_time(payload)

                if not text:
                    continue

                if issued_dt is None:
                    print(f"[{office}] product {product_id} missing issued time; skipping")
                    continue

                all_reports.extend(parse_lsr_text(text, office, issued_dt))
                time.sleep(0.15)

            except Exception as exc:
                print(f"[{office}] failed to parse product {product_id}: {exc}")

    all_reports = dedupe_reports(all_reports)
    reports, window_start, window_end = filter_last_24_hours_by_issued_time(all_reports)
    reports, new_reports_since_last_update = mark_new_reports(reports, previous_payload)

    return {
        "generated_at": window_end.isoformat(timespec="seconds"),
        "previous_generated_at": previous_payload.get("generated_at"),
        "window_start": window_start.isoformat(timespec="seconds"),
        "window_end": window_end.isoformat(timespec="seconds"),
        "new_reports_since_last_update": new_reports_since_last_update,
        "reports": reports,
    }


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    previous_payload = load_previous_payload()
    payload = build_payload(previous_payload)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {len(payload['reports'])} reports to {OUTPUT_FILE}")
    print(f"New reports since last update: {payload['new_reports_since_last_update']}")


if __name__ == "__main__":
    main()
