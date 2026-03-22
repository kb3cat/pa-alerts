#!/usr/bin/env python3

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode

import requests


OUTPUT_PATH = Path("data/storm_events_recent_pa.json")
USER_AGENT = "PennAlertsRecentStorms/1.0 (your-email@example.com)"
IEM_LSR_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/gis/lsr.py"

ROLLING_DAYS = 120


@dataclass
class StormEventRecent:
    id: str
    source: str
    status: str
    office: str | None
    event_time: str | None
    end_time: str | None
    county: str | None
    municipality: str | None
    state: str | None
    event_type: str | None
    magnitude: str | None
    description: str | None
    episode_narrative: str | None
    event_narrative: str | None
    lat: float | None
    lon: float | None
    damage_property: str | None
    damage_crops: str | None
    injuries_direct: int | None
    injuries_indirect: int | None
    deaths_direct: int | None
    deaths_indirect: int | None
    source_detail: str | None
    episode_id: str | None
    event_id: str | None
    cz_type: str | None
    cz_name: str | None
    magnitude_raw: str | None
    magnitude_type: str | None
    flood_cause: str | None
    tor_f_scale: str | None
    tor_length: str | None
    tor_width: str | None
    tor_other_wfo: str | None
    begin_location: str | None
    end_location: str | None
    begin_yomon: str | None
    year: str | None
    month_name: str | None
    keywords: list[str]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def norm(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def clean_dash(value: object) -> str | None:
    s = norm(value)
    if not s or s in {"--", "—"}:
        return None
    return s


def title_case(value: object) -> str | None:
    s = clean_dash(value)
    if not s:
        return None
    return " ".join(word.capitalize() if word.isupper() else word for word in s.split())


def parse_float(value: object) -> float | None:
    s = clean_dash(value)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_int(value: object) -> int | None:
    s = clean_dash(value)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def looks_like_zone_name(value: str | None) -> bool:
    if not value:
        return False
    v = value.lower()
    zone_words = [
        "ridges", "mountains", "highlands", "lowlands",
        "valley", "valleys", "uplands",
        "county", "counties", "zone", "zones"
    ]
    return any(word in v for word in zone_words)


def clean_county(value: object) -> str | None:
    s = clean_dash(value)
    if not s:
        return None
    s = title_case(s)
    if not s:
        return None
    s = re.sub(r"\s+County$", "", s, flags=re.IGNORECASE)
    return s


def clean_municipality(location: object, county: str | None) -> str | None:
    loc = clean_dash(location)
    if not loc:
        return None

    loc = title_case(loc)
    if not loc:
        return None

    if county and loc.lower() == county.lower():
        return None

    if looks_like_zone_name(loc):
        return None

    return loc


def make_keywords(*parts: object) -> list[str]:
    text = " ".join(norm(p) for p in parts if clean_dash(p))
    if not text:
        return []

    pieces = re.split(r"[,;/]| and | near | on | at | with ", text, flags=re.IGNORECASE)
    out: list[str] = []
    seen: set[str] = set()

    for piece in pieces:
        token = norm(piece).lower()
        if len(token) < 3:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)

    return out[:20]


def build_request_url() -> str:
    end_dt = now_utc()
    start_dt = end_dt - timedelta(days=ROLLING_DAYS)

    params = {
        "state": "PA",
        "sts": start_dt.strftime("%Y-%m-%dT%H:%MZ"),
        "ets": end_dt.strftime("%Y-%m-%dT%H:%MZ"),
        "fmt": "csv",
    }
    return f"{IEM_LSR_URL}?{urlencode(params)}"


def fetch_csv_text() -> str:
    url = build_request_url()
    r = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "text/csv,*/*"},
        timeout=120,
    )
    r.raise_for_status()
    return r.text


def parse_event_time(value: str | None) -> datetime | None:
    s = clean_dash(value)
    if not s:
        return None

    for candidate in (s.replace("Z", "+00:00"), s):
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    formats = [
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M %p",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue

    return None


def row_get(row: dict[str, str], *names: str) -> str | None:
    lower_map = {k.lower(): v for k, v in row.items()}
    for name in names:
        if name.lower() in lower_map:
            return lower_map[name.lower()]
    return None


def normalize_row(row: dict[str, str], seq: int) -> StormEventRecent | None:
    state_raw = row_get(row, "state", "st")
    if norm(state_raw).upper() not in {"PA", "PENNSYLVANIA"}:
        return None

    event_dt = parse_event_time(
        row_get(row, "valid", "utcvalid", "issue", "time", "datetime")
    )
    if event_dt is None:
        return None

    county = clean_county(row_get(row, "county", "county_name"))
    location = row_get(row, "city", "location", "town", "remark_location")
    municipality = clean_municipality(location, county)

    event_type = title_case(row_get(row, "type", "typetext", "eventtype", "phenomena"))
    magnitude_raw = clean_dash(row_get(row, "magnitude", "mag"))
    magnitude_type = clean_dash(row_get(row, "magnitude_units", "mag_units", "unit"))

    magnitude = None
    if magnitude_raw and magnitude_type:
        magnitude = f"{magnitude_raw} {magnitude_type}"
    elif magnitude_raw:
        magnitude = magnitude_raw

    description = clean_dash(row_get(row, "remark", "comments", "comment", "text")) or event_type
    office = clean_dash(row_get(row, "wfo"))
    lat = parse_float(row_get(row, "lat", "latitude"))
    lon = parse_float(row_get(row, "lon", "longitude"))

    year = str(event_dt.year)
    month_name = event_dt.strftime("%B")
    begin_yomon = event_dt.strftime("%Y%m")

    provided_id = clean_dash(row_get(row, "id", "lsr_id", "report_id"))
    rid = provided_id or f"recent-{event_dt.strftime('%Y%m%d%H%M')}-{seq:06d}"

    return StormEventRecent(
        id=rid,
        source="NWS_LSR",
        status="preliminary",
        office=office,
        event_time=iso_z(event_dt),
        end_time=None,
        county=county,
        municipality=municipality,
        state="PA",
        event_type=event_type,
        magnitude=magnitude,
        description=description,
        episode_narrative=None,
        event_narrative=description,
        lat=lat,
        lon=lon,
        damage_property=None,
        damage_crops=None,
        injuries_direct=None,
        injuries_indirect=None,
        deaths_direct=None,
        deaths_indirect=None,
        source_detail=clean_dash(row_get(row, "source")),
        episode_id=None,
        event_id=None,
        cz_type=None,
        cz_name=None,
        magnitude_raw=magnitude_raw,
        magnitude_type=magnitude_type,
        flood_cause=None,
        tor_f_scale=clean_dash(row_get(row, "f_scale", "tor_f_scale")),
        tor_length=None,
        tor_width=None,
        tor_other_wfo=None,
        begin_location=title_case(location),
        end_location=None,
        begin_yomon=begin_yomon,
        year=year,
        month_name=month_name,
        keywords=make_keywords(
            event_type,
            county,
            municipality,
            description,
            office,
        ),
    )


def dedupe_recent(items: list[StormEventRecent]) -> list[StormEventRecent]:
    out: list[StormEventRecent] = []
    seen: set[str] = set()

    for item in items:
        key = "|".join([
            norm(item.source),
            norm(item.office),
            norm(item.event_time),
            norm(item.county),
            norm(item.municipality),
            norm(item.event_type),
            norm(item.description),
        ]).lower()

        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def main() -> int:
    csv_text = fetch_csv_text()

    reader = csv.DictReader(StringIO(csv_text))
    events: list[StormEventRecent] = []

    for i, row in enumerate(reader, start=1):
        item = normalize_row(row, i)
        if item is not None:
            events.append(item)

    events = dedupe_recent(events)
    events.sort(key=lambda x: (x.event_time or "", x.id), reverse=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "storm_events_recent_pa",
        "fetched_at": iso_z(now_utc()),
        "count": len(events),
        "items": [asdict(x) for x in events],
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Final recent rows: {len(events)}")
    print(f"Wrote: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
