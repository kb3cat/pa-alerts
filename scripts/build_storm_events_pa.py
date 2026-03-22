#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import requests


DEFAULT_OUTPUT = Path("data/storm_events_pa.json")
DEFAULT_USER_AGENT = "PennAlertsStormHistory/1.0 (your-email@example.com)"


@dataclass
class StormEvent:
    id: str
    source: str
    status: str
    office: str | None
    issued_at: str | None
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
    begin_range_miles: str | None
    begin_azimuth: str | None
    end_range_miles: str | None
    end_azimuth: str | None
    begin_yomon: str | None
    year: str | None
    month_name: str | None
    keywords: list[str]


def norm_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def title_case(value: object) -> str | None:
    s = norm_space(value)
    if not s:
        return None
    return " ".join(word.capitalize() if word.isupper() else word for word in s.split())


def clean_upper_state_to_abbr(value: object) -> str | None:
    s = norm_space(value).upper()
    if not s:
        return None
    if s == "PENNSYLVANIA":
        return "PA"
    return s


def parse_float(value: object) -> float | None:
    s = norm_space(value)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def parse_int(value: object) -> int | None:
    s = norm_space(value)
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_damage(value: object) -> str | None:
    s = norm_space(value)
    return s or None


def zero_pad_time(value: object) -> str | None:
    s = re.sub(r"[^\d]", "", norm_space(value))
    if not s:
        return None
    return s.zfill(4)


def build_dt(yearmonth: object, day: object, hhmm: object) -> datetime | None:
    ym = norm_space(yearmonth)
    dd = norm_space(day)
    tm = zero_pad_time(hhmm)

    if not ym or not dd or not tm or len(ym) != 6:
        return None

    try:
        year = int(ym[:4])
        month = int(ym[4:6])
        day_num = int(dd)
        hour = int(tm[:2])
        minute = int(tm[2:4])

        # NCEI local times do not include timezone in the CSV.
        # For consistency in your board dataset, store them as UTC-tagged timestamps.
        return datetime(year, month, day_num, hour, minute, tzinfo=timezone.utc)
    except Exception:
        return None


def make_keywords(*parts: object) -> list[str]:
    text = " ".join(norm_space(p) for p in parts if norm_space(p))
    if not text:
        return []

    fragments = re.split(r"[,;/]| and | near | on | at | with ", text, flags=re.IGNORECASE)
    seen: set[str] = set()
    out: list[str] = []

    for frag in fragments:
        token = norm_space(frag).lower()
        if len(token) < 3:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)

    return out[:20]


def combine_description(event_narrative: str | None, episode_narrative: str | None, fallback: str | None) -> str | None:
    if norm_space(event_narrative):
        return norm_space(event_narrative)
    if norm_space(episode_narrative):
        return norm_space(episode_narrative)
    return norm_space(fallback) or None


def format_magnitude(mag: object, mag_type: object) -> str | None:
    m = norm_space(mag)
    mt = norm_space(mag_type)
    if m and mt:
        return f"{m} {mt}"
    if m:
        return m
    return None


def open_csv_bytes_from_url(url: str, user_agent: str) -> bytes:
    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
    }
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    return r.content


def open_csv_bytes_from_file(path: Path) -> bytes:
    return path.read_bytes()


def decode_maybe_gzip(content: bytes, source_name: str) -> str:
    lower = source_name.lower()
    if lower.endswith(".gz"):
        return gzip.decompress(content).decode("utf-8", errors="replace")

    # fallback magic check
    if len(content) >= 2 and content[:2] == b"\x1f\x8b":
        return gzip.decompress(content).decode("utf-8", errors="replace")

    return content.decode("utf-8", errors="replace")


def iter_rows_from_text(text: str) -> Iterable[dict[str, str]]:
    buffer = io.StringIO(text)
    reader = csv.DictReader(buffer)
    for row in reader:
        yield {str(k or "").strip(): str(v or "").strip() for k, v in row.items()}


def normalize_row(row: dict[str, str]) -> StormEvent | None:
    state_raw = norm_space(row.get("STATE"))
    if state_raw.upper() != "PENNSYLVANIA":
        return None

    event_id = norm_space(row.get("EVENT_ID")) or None
    episode_id = norm_space(row.get("EPISODE_ID")) or None

    begin_dt = build_dt(
        row.get("BEGIN_YEARMONTH"),
        row.get("BEGIN_DAY"),
        row.get("BEGIN_TIME"),
    )
    end_dt = build_dt(
        row.get("END_YEARMONTH"),
        row.get("END_DAY"),
        row.get("END_TIME"),
    )

    county = title_case(row.get("COUNTY"))
    cz_name = title_case(row.get("CZ_NAME"))
    event_type = title_case(row.get("EVENT_TYPE"))
    office = norm_space(row.get("WFO")) or None

    event_narrative = norm_space(row.get("EVENT_NARRATIVE")) or None
    episode_narrative = norm_space(row.get("EPISODE_NARRATIVE")) or None

    description = combine_description(
        event_narrative,
        episode_narrative,
        event_type,
    )

    lat = parse_float(row.get("BEGIN_LAT"))
    lon = parse_float(row.get("BEGIN_LON"))

    magnitude = format_magnitude(row.get("MAGNITUDE"), row.get("MAGNITUDE_TYPE"))

    rid = f"{norm_space(row.get('YEAR')) or '0000'}-{event_id or episode_id or 'unknown'}"

    begin_location = title_case(row.get("BEGIN_LOCATION"))
    end_location = title_case(row.get("END_LOCATION"))

    municipality = begin_location or cz_name

    return StormEvent(
        id=rid,
        source="NCEI",
        status="official",
        office=office,
        issued_at=None,
        event_time=iso_z(begin_dt),
        end_time=iso_z(end_dt),
        county=county,
        municipality=municipality,
        state=clean_upper_state_to_abbr(state_raw),
        event_type=event_type,
        magnitude=magnitude,
        description=description,
        episode_narrative=episode_narrative,
        event_narrative=event_narrative,
        lat=lat,
        lon=lon,
        damage_property=parse_damage(row.get("DAMAGE_PROPERTY")),
        damage_crops=parse_damage(row.get("DAMAGE_CROPS")),
        injuries_direct=parse_int(row.get("INJURIES_DIRECT")),
        injuries_indirect=parse_int(row.get("INJURIES_INDIRECT")),
        deaths_direct=parse_int(row.get("DEATHS_DIRECT")),
        deaths_indirect=parse_int(row.get("DEATHS_INDIRECT")),
        source_detail=norm_space(row.get("SOURCE")) or None,
        episode_id=episode_id,
        event_id=event_id,
        cz_type=norm_space(row.get("CZ_TYPE")) or None,
        cz_name=cz_name,
        magnitude_raw=norm_space(row.get("MAGNITUDE")) or None,
        magnitude_type=norm_space(row.get("MAGNITUDE_TYPE")) or None,
        flood_cause=norm_space(row.get("FLOOD_CAUSE")) or None,
        tor_f_scale=norm_space(row.get("TOR_F_SCALE")) or None,
        tor_length=norm_space(row.get("TOR_LENGTH")) or None,
        tor_width=norm_space(row.get("TOR_WIDTH")) or None,
        tor_other_wfo=norm_space(row.get("TOR_OTHER_WFO")) or None,
        begin_location=begin_location,
        end_location=end_location,
        begin_range_miles=norm_space(row.get("BEGIN_RANGE")) or None,
        begin_azimuth=norm_space(row.get("BEGIN_AZIMUTH")) or None,
        end_range_miles=norm_space(row.get("END_RANGE")) or None,
        end_azimuth=norm_space(row.get("END_AZIMUTH")) or None,
        begin_yomon=norm_space(row.get("BEGIN_YEARMONTH")) or None,
        year=norm_space(row.get("YEAR")) or None,
        month_name=norm_space(row.get("MONTH_NAME")) or None,
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


def dedupe_events(items: list[StormEvent]) -> list[StormEvent]:
    seen: set[str] = set()
    out: list[StormEvent] = []

    for item in items:
        key = "|".join([
            norm_space(item.event_id),
            norm_space(item.episode_id),
            norm_space(item.event_time),
            norm_space(item.county),
            norm_space(item.event_type),
            norm_space(item.description),
        ]).lower()

        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def sort_events(items: list[StormEvent]) -> list[StormEvent]:
    return sorted(
        items,
        key=lambda x: (x.event_time or "", x.event_id or ""),
        reverse=True,
    )


def parse_source_arg(value: str) -> tuple[str, str]:
    """
    Returns:
      ("url", "https://...")
      ("file", "/path/to/file.csv.gz")
    """
    if value.startswith("http://") or value.startswith("https://"):
        return ("url", value)
    return ("file", value)


def load_source_text(source_value: str, user_agent: str) -> tuple[str, str]:
    source_type, source_ref = parse_source_arg(source_value)

    if source_type == "url":
        raw = open_csv_bytes_from_url(source_ref, user_agent=user_agent)
        return decode_maybe_gzip(raw, source_ref), source_ref

    path = Path(source_ref)
    raw = open_csv_bytes_from_file(path)
    return decode_maybe_gzip(raw, path.name), str(path)


def write_output(path: Path, events: list[StormEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "name": "storm_events_pa",
        "fetched_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "count": len(events),
        "items": [asdict(x) for x in events],
    }

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[OK] Wrote {len(events)} Pennsylvania storm events to {path}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Build Pennsylvania storm events JSON from NCEI StormEvents_details CSV/CSV.GZ files"
    )
    p.add_argument(
        "--source",
        action="append",
        required=True,
        help="Input source. Can be a local file path or direct URL. Repeat --source for multiple files.",
    )
    p.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Output JSON path",
    )
    p.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent string for HTTP downloads",
    )
    p.add_argument(
        "--min-year",
        type=int,
        default=None,
        help="Optional minimum YEAR to keep",
    )
    p.add_argument(
        "--event-type",
        action="append",
        default=[],
        help="Optional event type filter, repeatable, e.g. --event-type Tornado",
    )
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    all_events: list[StormEvent] = []
    allowed_event_types = {norm_space(x).lower() for x in args.event_type if norm_space(x)}

    for src in args.source:
        try:
            text, label = load_source_text(src, user_agent=args.user_agent)
            print(f"[INFO] Loaded source: {label}")
        except Exception as exc:
            print(f"[ERROR] Failed loading {src}: {exc}", file=sys.stderr)
            return 1

        count_rows = 0
        count_pa = 0

        for row in iter_rows_from_text(text):
            count_rows += 1
            item = normalize_row(row)
            if item is None:
                continue

            if args.min_year is not None:
                try:
                    if item.year is None or int(item.year) < args.min_year:
                        continue
                except Exception:
                    continue

            if allowed_event_types:
                if norm_space(item.event_type).lower() not in allowed_event_types:
                    continue

            count_pa += 1
            all_events.append(item)

        print(f"[INFO] Parsed {count_rows} rows, kept {count_pa} PA rows from {label}")

    all_events = dedupe_events(all_events)
    all_events = sort_events(all_events)

    write_output(Path(args.output), all_events)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
