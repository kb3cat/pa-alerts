#!/usr/bin/env python3

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
class StormEvent:
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
    if not s or s in {"--", "—", "N/A", "n/a", "NULL", "null", "None"}:
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


def parse_datetime(value: object) -> datetime | None:
    s = clean_dash(value)
    if not s:
        return None

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y%m%d%H%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def make_event_dt(row: dict[str, str], prefix: str) -> datetime | None:
    y = clean_dash(row.get(f"BEGIN_YEARMONTH")) if prefix == "BEGIN" else clean_dash(row.get("END_YEARMONTH"))
    day = clean_dash(row.get(f"{prefix}_DAY"))
    tm = clean_dash(row.get(f"{prefix}_TIME"))

    if not y or not day or not tm:
        return None

    try:
        y = str(y)
        day_num = int(day)
        tm_num = int(tm)
        year = int(y[:4])
        month = int(y[4:6])
        hour = tm_num // 100
        minute = tm_num % 100
        return datetime(year, month, day_num, hour, minute, tzinfo=timezone.utc)
    except Exception:
        return None


def looks_like_zone_name(value: str | None) -> bool:
    if not value:
        return False
    v = value.lower()
    zone_words = [
        "ridges", "mountains", "highlands", "lowlands",
        "valley", "valleys", "uplands",
        "county", "counties", "zone", "zones",
    ]
    return any(word in v for word in zone_words)


def clean_county(value: object) -> str | None:
    s = clean_dash(value)
    if not s:
        return None

    s = title_case(s)
    if not s:
        return None

    # Remove directional prefixes
    s = re.sub(
        r"^(North|South|East|West|Northern|Southern|Eastern|Western|Central)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )

    # Remove terrain/zone descriptors
    s = re.sub(
        r"^(Higher Elevations Of|Lower Elevations Of|Ridges Of|Mountains Of)\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )

    # Remove standalone terrain suffixes
    s = re.sub(
        r"\s+(Ridges|Mountains|Highlands|Lowlands)$",
        "",
        s,
        flags=re.IGNORECASE,
    )

    # Remove "County"
    s = re.sub(r"\s+County$", "", s, flags=re.IGNORECASE)

    return s.strip()


def clean_municipality(value: object, county: str | None) -> str | None:
    s = clean_dash(value)
    if not s:
        return None

    s = title_case(s)
    if not s:
        return None

    if county and s.lower() == county.lower():
        return None

    if looks_like_zone_name(s):
        return None

    return s


def format_magnitude(raw: str | None, units: str | None) -> str | None:
    if raw and units:
        return f"{raw} {units}"
    return raw


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


def normalize_row(row: dict[str, str]) -> StormEvent | None:
    state = clean_dash(row.get("STATE"))
    if norm(state).upper() not in {"PA", "PENNSYLVANIA"}:
        return None

    begin_dt = make_event_dt(row, "BEGIN")
    end_dt = make_event_dt(row, "END")

    event_id = clean_dash(row.get("EVENT_ID"))
    episode_id = clean_dash(row.get("EPISODE_ID"))

    county = clean_county(row.get("CZ_NAME") or row.get("COUNTY") or row.get("COUNTY_NAME"))
    municipality = clean_municipality(row.get("BEGIN_LOCATION"), county)

    event_type = title_case(row.get("EVENT_TYPE"))
    magnitude_raw = clean_dash(row.get("MAGNITUDE"))
    magnitude_type = clean_dash(row.get("MAGNITUDE_TYPE"))
    magnitude = format_magnitude(magnitude_raw, magnitude_type)

    episode_narrative = clean_dash(row.get("EPISODE_NARRATIVE"))
    event_narrative = clean_dash(row.get("EVENT_NARRATIVE"))
    description = event_narrative or episode_narrative or event_type

    lat = parse_float(row.get("BEGIN_LAT"))
    lon = parse_float(row.get("BEGIN_LON"))

    return StormEvent(
        id=event_id or f"ncei-{episode_id or 'unknown'}-{row.get('BEGIN_YEARMONTH', '')}-{row.get('BEGIN_DAY', '')}",
        source="NCEI_STORM_EVENTS",
        status="official",
        office=clean_dash(row.get("WFO")),
        event_time=iso_z(begin_dt),
        end_time=iso_z(end_dt),
        county=county,
        municipality=municipality,
        state="PA",
        event_type=event_type,
        magnitude=magnitude,
        description=description,
        episode_narrative=episode_narrative,
        event_narrative=event_narrative,
        lat=lat,
        lon=lon,
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
        cz_name=clean_dash(row.get("CZ_NAME")),
        magnitude_raw=magnitude_raw,
        magnitude_type=magnitude_type,
        flood_cause=clean_dash(row.get("FLOOD_CAUSE")),
        tor_f_scale=clean_dash(row.get("TOR_F_SCALE")),
        tor_length=clean_dash(row.get("TOR_LENGTH")),
        tor_width=clean_dash(row.get("TOR_WIDTH")),
        tor_other_wfo=clean_dash(row.get("TOR_OTHER_WFO")),
        begin_location=title_case(row.get("BEGIN_LOCATION")),
        end_location=title_case(row.get("END_LOCATION")),
        begin_yomon=clean_dash(row.get("BEGIN_YEARMONTH")),
        year=clean_dash(str(begin_dt.year)) if begin_dt else None,
        month_name=begin_dt.strftime("%B") if begin_dt else None,
        keywords=make_keywords(
            event_type,
            county,
            municipality,
            description,
            row.get("WFO"),
        ),
    )


def dedupe_items(items: list[StormEvent]) -> list[StormEvent]:
    out: list[StormEvent] = []
    seen: set[str] = set()

    for item in items:
        key = "|".join([
            norm(item.source),
            norm(item.event_time),
            norm(item.county),
            norm(item.municipality),
            norm(item.event_type),
            norm(item.description),
            norm(item.event_id),
        ]).lower()

        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def main() -> int:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing input file: {INPUT_PATH}")

    items: list[StormEvent] = []

    with gzip.open(INPUT_PATH, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            item = normalize_row(row)
            if item is not None:
                items.append(item)

    items = dedupe_items(items)
    items.sort(key=lambda x: (x.event_time or "", x.id), reverse=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "name": "storm_events_pa",
        "fetched_at": iso_z(now_utc()),
        "count": len(items),
        "items": [asdict(x) for x in items],
    }

    OUTPUT_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Final historical rows: {len(items)}")
    print(f"Wrote: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
