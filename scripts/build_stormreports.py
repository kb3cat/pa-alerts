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
    "PIT": "Pittsburgh",
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

ROW2_RE = re.compile(
    r"^(?P<date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<mag>.{1,16}?)\s+"
    r"(?P<county>.{1,18}?)\s+"
    r"(?P<state>[A-Z]{2})\s+"
    r"(?P<source>.{1,18}?)\s*$"
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


def fetch_product_text(product_id: str) -> str:
    url = f"{API_BASE}/products/{product_id}"
    payload = fetch_json(url)
    return payload.get("productText", "") or payload.get("text", "")


def parse_lsr_text(text: str, office: str) -> list[dict]:
    lines = text.splitlines()
    results = []
    idx = 0
    row_num = 0

    while idx < len(lines) - 1:
        line1 = lines[idx].rstrip()
        line2 = lines[idx + 1].rstrip()

        m1 = ROW1_RE.match(line1)
        m2 = ROW2_RE.match(line2)

        if not (m1 and m2):
            idx += 1
            continue

        row_num += 1

        time_str = clean_spaces(m1.group("time"))
        event = clean_spaces(m1.group("event"))
        location = clean_spaces(m1.group("location"))
        lat = latlon_to_float(m1.group("lat"))
        lon = latlon_to_float(m1.group("lon"))

        date_str = clean_spaces(m2.group("date"))
        magnitude = clean_spaces(m2.group("mag"))
        county = clean_spaces(m2.group("county"))
        state = clean_spaces(m2.group("state"))
        source = clean_spaces(m2.group("source"))

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
            "id": f"{office}-{report_dt.strftime('%Y%m%d-%H%M')}-{row_num}",
            "office": office,
            "office_name": OFFICES[office],
            "event": event,
            "magnitude": "" if magnitude.upper() == "UNK" else magnitude,
            "location": location,
            "county": county,
            "source": source,
            "remarks": remarks,
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
            report["report_time"],
            report["lat"],
            report["lon"],
        )
        if key in seen:
            continue
        seen.add(key)
        output.append(report)

    return output


def filter_last_24_hours(reports: list[dict]) -> tuple[list[dict], datetime, datetime]:
    now_local = datetime.now(LOCAL_TZ)
    window_start = now_local - timedelta(hours=24)

    filtered = []
    for report in reports:
        try:
            dt = datetime.fromisoformat(report["report_time"])
        except Exception:
            continue
        if window_start <= dt <= now_local:
            filtered.append(report)

    filtered.sort(key=lambda r: r["report_time"], reverse=True)
    return filtered, window_start, now_local


def build_payload() -> dict:
    all_reports = []

    for office in OFFICES:
        try:
            product_ids = get_product_ids_for_office(office)
        except Exception as exc:
            print(f"[{office}] failed to fetch product list: {exc}")
            continue

        for product_id in product_ids:
            try:
                text = fetch_product_text(product_id)
                if text:
                    all_reports.extend(parse_lsr_text(text, office))
                time.sleep(0.15)
            except Exception as exc:
                print(f"[{office}] failed to parse product {product_id}: {exc}")

    all_reports = dedupe_reports(all_reports)
    reports, window_start, window_end = filter_last_24_hours(all_reports)

    return {
        "generated_at": window_end.isoformat(timespec="seconds"),
        "window_start": window_start.isoformat(timespec="seconds"),
        "window_end": window_end.isoformat(timespec="seconds"),
        "reports": reports,
    }


def main() -> None:
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    payload = build_payload()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote {len(payload['reports'])} reports to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
