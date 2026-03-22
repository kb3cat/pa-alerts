#!/usr/bin/env python3

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


OUTPUT_PATH = Path("data/storm_events_recent_pa.json")

# PA-serving / bordering WFOs that commonly issue PA-related LSRs
OFFICES = ["CTP", "PBZ", "PHI", "BGM", "BUF", "CLE"]

NWS_API_BASE = "https://api.weather.gov"
USER_AGENT = "PennAlertsRecentStorms/1.0 (your-email@example.com)"

# How many recent LSR products per office to inspect
MAX_PRODUCTS_PER_OFFICE = 8

# Only keep recent events newer than this many days
RECENT_DAYS = 120


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


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/ld+json, application/json, text/plain;q=0.9, */*;q=0.8",
    })
    return s


def get_json(session: requests.Session, url: str) -> dict:
    r = session.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def fetch_lsr_index(session: requests.Session, office: str) -> list[dict]:
    url = f"{NWS_API_BASE}/products/types/LSR/locations/{office}"
    data = get_json(session, url)

    if "@graph" in data and isinstance(data["@graph"], list):
        return data["@graph"]

    if "products" in data and isinstance(data["products"], list):
        return data["products"]

    return []


def extract_product_id(item: dict) -> str | None:
    for key in ("id", "@id"):
        val = item.get(key)
        if isinstance(val, str):
            m = re.search(r"/products/([A-Za-z0-9\-]+)$", val)
            if m:
                return m.group(1)
            if re.fullmatch(r"[A-Za-z0-9\-]+", val):
                return val
    return None


def fetch_product_text(session: requests.Session, product_id: str) -> tuple[str, str | None]:
    url = f"{NWS_API_BASE}/products/{product_id}"
    data = get_json(session, url)
    text = (
        data.get("productText")
        or data.get("text")
        or data.get("productTextFormatted")
        or ""
    )
    issued = data.get("issuanceTime") or data.get("issued")
    return str(text), issued


TIME_LINE_RE = re.compile(
    r"^\s*(\d{3,4}\s[AP]M)\s+(.+?)\s{2,}(.+?)\s+(\d{4,5})\s+(\d{4,5})\s*$"
)

DATE_LINE_RE = re.compile(
    r"^\s*(\d{2}/\d{2}/\d{4})\s+(.+?)\s{2,}(.+?)\s+([A-Z]{2})\s+(.+?)\s*$"
)


def parse_lsr_blocks(product_text: str) -> list[list[str]]:
    lines = [ln.rstrip() for ln in product_text.splitlines()]

    blocks: list[list[str]] = []
    current: list[str] = []

    in_body = False
    for ln in lines:
        stripped = ln.strip()

        if stripped.startswith("..TIME...") or stripped.startswith("TIME..."):
            in_body = True
            if current:
                current = []
            continue

        if not in_body:
            continue

        if TIME_LINE_RE.match(ln):
            if current:
                blocks.append(current)
            current = [ln]
            continue

        if current and stripped:
            current.append(ln)

    if current:
        blocks.append(current)

    return blocks


def parse_lsr_datetime(local_time_text: str, date_text: str, issued_at: str | None) -> datetime | None:
    """
    LSR event times are office-local wall times.
    To avoid wrong date ordering around midnight, use the issued_at date as a reference,
    but still store as UTC-tagged naive conversion for consistency with the rest of the board.
    """
    try:
        base = datetime.strptime(f"{date_text} {local_time_text}", "%m/%d/%Y %I%M %p")
    except Exception:
        return None

    if issued_at:
        try:
            issued_dt = datetime.fromisoformat(issued_at.replace("Z", "+00:00"))
            # Keep date from parsed line; no timezone mapping per office here.
            return base.replace(tzinfo=timezone.utc)
        except Exception:
            pass

    return base.replace(tzinfo=timezone.utc)


def parse_latlon_pair(lat_str: str | None, lon_str: str | None) -> tuple[float | None, float | None]:
    if not lat_str or not lon_str:
        return None, None

    lat_digits = re.sub(r"[^\d]", "", lat_str)
    lon_digits = re.sub(r"[^\d]", "", lon_str)

    if len(lat_digits) < 4 or len(lon_digits) < 4:
        return None, None

    try:
        lat = float(f"{lat_digits[:-2]}.{lat_digits[-2:]}")
        lon = -float(f"{lon_digits[:-2]}.{lon_digits[-2:]}")
        return lat, lon
    except Exception:
        return None, None


def parse_magnitude_and_source(raw_magnitude: str | None, source_line: str | None) -> tuple[str | None, str | None, str | None]:
    magnitude_raw = clean_dash(raw_magnitude)
    source_detail = clean_dash(source_line)

    magnitude = magnitude_raw
    magnitude_type = None

    if magnitude_raw:
        # Light attempt to split "60 MPH" or "1.00 INCH"
        m = re.match(r"^(.*?)(?:\s+([A-Z%/]+))?$", magnitude_raw.strip())
        if m:
            mag_val = clean_dash(m.group(1))
            mag_type = clean_dash(m.group(2))
            if mag_val and mag_type:
                magnitude = f"{mag_val} {mag_type}"
                magnitude_raw = mag_val
                magnitude_type = mag_type

    return magnitude, magnitude_raw, magnitude_type


def parse_lsr_report_block(block: list[str], office: str, issued_at: str | None, seq: int) -> StormEventRecent | None:
    if len(block) < 2:
        return None

    line1 = block[0]
    line2 = block[1]

    m1 = TIME_LINE_RE.match(line1)
    m2 = DATE_LINE_RE.match(line2)

    if not m1 or not m2:
        return None

    local_time_text = norm(m1.group(1))
    event_type = title_case(m1.group(2))
    location_text = title_case(m1.group(3))
    lat_raw = m1.group(4)
    lon_raw = m1.group(5)

    date_text = norm(m2.group(1))
    raw_magnitude = norm(m2.group(2))
    county_raw = norm(m2.group(3))
    state = norm(m2.group(4)).upper()
    source_line = norm(m2.group(5))

    if state != "PA":
        return None

    county = clean_county(county_raw)
    municipality = clean_municipality(location_text, county)

    remarks = " ".join(norm(x) for x in block[2:]).strip()
    description = clean_dash(remarks) or clean_dash(source_line) or event_type

    lat, lon = parse_latlon_pair(lat_raw, lon_raw)
    event_dt = parse_lsr_datetime(local_time_text, date_text, issued_at)

    magnitude, magnitude_raw, magnitude_type = parse_magnitude_and_source(raw_magnitude, source_line)

    year = date_text[:4] if len(date_text) >= 10 else None
    month_name = None
    try:
        dt_tmp = datetime.strptime(date_text, "%m/%d/%Y")
        year = str(dt_tmp.year)
        month_name = dt_tmp.strftime("%B")
        begin_yomon = dt_tmp.strftime("%Y%m")
    except Exception:
        begin_yomon = None

    rid = f"recent-{date_text.replace('/', '')}-{office.lower()}-{seq:04d}"

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
        source_detail=clean_dash(source_line),
        episode_id=None,
        event_id=None,
        cz_type=None,
        cz_name=None,
        magnitude_raw=magnitude_raw,
        magnitude_type=magnitude_type,
        flood_cause=None,
        tor_f_scale=None,
        tor_length=None,
        tor_width=None,
        tor_other_wfo=None,
        begin_location=location_text,
        end_location=None,
        begin_yomon=begin_yomon,
        year=year,
        month_name=month_name,
        keywords=make_keywords(
            event_type,
            county,
            municipality,
            source_line,
            remarks,
        ),
    )


def fetch_recent_reports() -> list[StormEventRecent]:
    session = build_session()
    reports: list[StormEventRecent] = []

    for office in OFFICES:
        try:
            idx = fetch_lsr_index(session, office)
        except Exception as e:
            print(f"[WARN] Failed LSR index for {office}: {e}")
            continue

        product_ids: list[str] = []
        for item in idx[:MAX_PRODUCTS_PER_OFFICE]:
            pid = extract_product_id(item)
            if pid:
                product_ids.append(pid)

        seq = 0
        for pid in product_ids:
            try:
                text, issued_at = fetch_product_text(session, pid)
            except Exception as e:
                print(f"[WARN] Failed LSR product {pid} ({office}): {e}")
                continue

            blocks = parse_lsr_blocks(text)
            for block in blocks:
                seq += 1
                report = parse_lsr_report_block(block, office, issued_at, seq)
                if report:
                    reports.append(report)

    return reports


def dedupe(items: list[StormEventRecent]) -> list[StormEventRecent]:
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


def keep_only_recent(items: list[StormEventRecent], days: int) -> list[StormEventRecent]:
    cutoff = now_utc() - timedelta(days=days)
    out: list[StormEventRecent] = []

    for item in items:
        if not item.event_time:
            continue
        try:
            dt = datetime.fromisoformat(item.event_time.replace("Z", "+00:00"))
        except Exception:
            continue

        if dt >= cutoff:
            out.append(item)

    return out


def main() -> int:
    events = fetch_recent_reports()
    events = dedupe(events)
    events = keep_only_recent(events, RECENT_DAYS)
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
