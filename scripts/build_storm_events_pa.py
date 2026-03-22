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


def title_case(value: object) -> str | None:
    s = norm(value)
    if not s:
        return None
    return " ".join(word.capitalize() if word.isupper() else word for word in s.split())


def parse_float(value: object) -> float | None:
    s = norm(value)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_int(value: object) -> int | None:
    s = norm(value)
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
    m = norm(mag)
    mt = norm(mag_type)
    if m and mt:
        return f"{m} {mt}"
    if m:
        return m
    return None


def choose_description(event_narrative: str | None, episode_narrative: str | None, fallback: str | None) -> str | None:
    if norm(event_narrative):
        return norm(event_narrative)
    if norm(episode_narrative):
        return norm(episode_narrative)
    return norm(fallback) or None


def make_keywords(*parts: object) -> list[str]:
    text = " ".join(norm(p) for p in parts if norm(p))
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

    county = title_case(row.get("COUNTY"))
    cz_name = title_case(row.get("CZ_NAME"))
    begin_location = title_case(row.get("BEGIN_LOCATION"))
    end_location = title_case(row.get("END_LOCATION"))
    municipality = begin_location or cz_name

    event_type = title_case(row.get("EVENT_TYPE"))
    event_narrative = norm(row.get("EVENT_NARRATIVE")) or None
    episode_narrative = norm(row.get("EPISODE_NARRATIVE")) or None
    description = choose_description(event_narrative, episode_narrative, event_type)

    event_id = norm(row.get("EVENT_ID")) or None
    episode_id = norm(row.get("EPISODE_ID")) or None
    year = norm(row.get("YEAR")) or None

    record_id = f"{year or '0000'}-{event_id or episode_id or 'unknown'}"

    return StormEventPA(
        id=record_id,
        source="NCEI",
        status="official",
        office=norm(row.get("WFO")) or None,
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
        damage_property=norm(row.get("DAMAGE_PROPERTY")) or None,
        damage_crops=norm(row.get("DAMAGE_CROPS")) or None,
        injuries_direct=parse_int(row.get("INJURIES_DIRECT")),
        injuries_indirect=parse_int(row.get("INJURIES_INDIRECT")),
        deaths_direct=parse_int(row.get("DEATHS_DIRECT")),
        deaths_indirect=parse_int(row.get("DEATHS_INDIRECT")),
        source_detail=norm(row.get("SOURCE")) or None,
        episode_id=episode_id,
        event_id=event_id,
        cz_type=norm(row.get("CZ_TYPE")) or None,
        cz_name=cz_name,
        magnitude_raw=norm(row.get("MAGNITUDE")) or None,
        magnitude_type=norm(row.get("MAGNITUDE_TYPE")) or None,
        flood_cause=norm(row.get("FLOOD_CAUSE")) or None,
        tor_f_scale=norm(row.get("TOR_F_SCALE")) or None,
        tor_length=norm(row.get("TOR_LENGTH")) or None,
        tor_width=norm(row.get("TOR_WIDTH")) or None,
        tor_other_wfo=norm(row.get("TOR_OTHER_WFO")) or None,
        begin_location=begin_location,
        end_location=end_location,
        begin_yomon=norm(row.get("BEGIN_YEARMONTH")) or None,
        year=year,
        month_name=norm(row.get("MONTH_NAME")) or None,
        keywords=make_keywords(
            row.get("EVENT_TYPE"),
            row.get("COUNTY"),
            row.get("CZ_NAME"),
            row.get("BEGIN_LOCATION"),
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
