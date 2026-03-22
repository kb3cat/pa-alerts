#!/usr/bin/env python3

from __future__ import annotations

import csv
import gzip
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


INPUT_PATH = Path("data/raw/StormEvents_details_latest.csv.gz")
OUTPUT_PATH = Path("data/storm_events_pa.json")


@dataclass
class StormEventPA:
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


def zero_pad_time(value: object) -> str | None:
    s = re.sub(r"[^\d]", "", norm(value))
    if not s:
        return None
    return s.zfill(4)


def build_dt(yearmonth: object, day: object, hhmm: object) -> datetime | None:
    ym = norm(yearmonth)
    dd = norm(day)
    tm = zero_pad_time(hhmm)

    if not ym or not dd or not tm or len(ym) != 6:
        return None

    try:
        year = int(ym[:4])
        month = int(ym[4:6])
        day_num = int(dd)
        hour = int(tm[:2])
        minute = int(tm[2:4])
        return datetime(year, month, day_num, hour, minute, tzinfo=timezone.utc)
    except Exception:
        return None


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def format_magnitude(mag: object, mag_type: object) -> str | None:
    m = clean_dash(mag)
    mt = clean_dash(mag_type)
    if m and mt:
        return f"{m} {mt}"
    if m:
        return m
    return None


def choose_description(event_narrative: str | None, episode_narrative: str | None, fallback: str | None) -> str | None:
    if clean_dash(event_narrative):
        return norm(event_narrative)
    if clean_dash(episode_narrative):
        return norm(episode_narrative)
    return clean_dash(fallback)


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


def clean_municipality(begin_location: object, county: str | None) -> str | None:
    loc = clean_dash(begin_location)
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


def normalize_row(row: dict[str, str]) -> StormEventPA | None:
    state = norm(row.get("STATE")).upper()
    if state != "PENNSYLVANIA":
        return None

    begin_dt = build_dt(row.get("BEGIN_YEARMONTH"), row.get("BEGIN_DAY"), row.get("BEGIN_TIME"))
    end_dt = build_dt(row.get("END_YEARMONTH"), row.get("END_DAY"), row.get("END_TIME"))

    county = clean_county(row.get("COUNTY"))
    cz_name = title_case(row.get("CZ_NAME"))
    begin_location = title_case(row.get("BEGIN_LOCATION"))
    end_location = title_case(row.get("END_LOCATION"))

    if not county and cz_name and not looks_like_zone_name(cz_name):
        county = re.sub(r"\s+County$", "", cz_name, flags=re.IGNORECASE)

    municipality = clean_municipality(row.get("BEGIN_LOCATION"), county)

    event_type = title_case(row.get("EVENT_TYPE"))
    event_narrative = clean_dash(row.get("EVENT_NARRATIVE"))
    episode_narrative = clean_dash(row.get("EPISODE_NARRATIVE"))
    description = choose_description(event_narrative, episode_narrative, event_type)

    event_id = clean_dash(row.get("EVENT_ID"))
    episode_id = clean_dash(row.get("EPISODE_ID"))
    year = clean_dash(row.get("YEAR"))

    record_id = f"{year or '0000'}-{event_id or episode_id or 'unknown'}"

    return StormEventPA(
        id=record_id,
        source="NCEI",
        status="official",
        office=clean_dash(row.get("WFO")),
        event_time=iso_z(begin_dt),
        end_time=iso_z(end_dt),
        county=county,
        municipality=municipality,
        state="PA",
        event_type=event_type,
        magnitude=format_magnitude(row.get("MAGNITUDE"), row.get("MAGNITUDE_TYPE")),
        description=description,
        episode_narrative=episode_narrative,
        event_narrative=event_narrative,
        lat=parse_float(row.get("BEGIN_LAT")),
        lon=parse_float(row.get("BEGIN_LON")),
        damage_property=clean_dash(row.get("DAMAGE_PROPERTY")),
        damage_crops=clean_dash(row.get("DAMAGE_CROPS")),
        injuries_direct=parse_int(row.get("INJURIES_DIRECT")),
        injuries_indirect=parse_int(row.get("INJURIES_INDIRECT")),
        deaths_direct=parse_int(row.get("DEATHS_DIRECT")),
        deaths_indirect=parse_int(row.get("DEATHS_INDIRECT")),
        source_detail=clean_dash(row.get("SOURCE")),
        episode_id=episode_id,
        event_id=event_id,
        cz_type=clean_dash(row.get("CZ_TYPE")),
        cz_name=cz_name,
        magnitude_raw=clean_dash(row.get("MAGNITUDE")),
        magnitude_type=clean_dash(row.get("MAGNITUDE_TYPE")),
        flood_cause=clean_dash(row.get("FLOOD_CAUSE")),
        tor_f_scale=clean_dash(row.get("TOR_F_SCALE")),
        tor_length=clean_dash(row.get("TOR_LENGTH")),
        tor_width=clean_dash(row.get("TOR_WIDTH")),
        tor_other_wfo=clean_dash(row.get("TOR_OTHER_WFO")),
        begin_location=begin_location,
        end_location=end_location,
        begin_yomon=clean_dash(row.get("BEGIN_YEARMONTH")),
        year=year,
        month_name=clean_dash(row.get("MONTH_NAME")),
        keywords=make_keywords(
            row.get("EVENT_TYPE"),
            county,
            begin_location,
            row.get("SOURCE"),
            row.get("EVENT_NARRATIVE"),
            row.get("EPISODE_NARRATIVE"),
        ),
    )


def dedupe(items: list[StormEventPA]) -> list[StormEventPA]:
    out: list[StormEventPA] = []
    seen: set[str] = set()

    for item in items:
        key = "|".join([
            norm(item.event_id),
            norm(item.episode_id),
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
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    events: list[StormEventPA] = []

    with gzip.open(INPUT_PATH, mode="rt", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        total_rows = 0
        kept_rows = 0

        for row in reader:
            total_rows += 1
            item = normalize_row(row)
            if item is None:
                continue
            kept_rows += 1
            events.append(item)

    events = dedupe(events)
    events.sort(key=lambda x: (x.event_time or "", x.event_id or ""), reverse=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "storm_events_pa",
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "count": len(events),
        "items": [asdict(x) for x in events],
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Read rows: {total_rows}")
    print(f"PA rows kept: {kept_rows}")
    print(f"Final deduped rows: {len(events)}")
    print(f"Wrote: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
