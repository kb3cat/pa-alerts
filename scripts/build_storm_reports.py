#!/usr/bin/env python3
"""
Build PennAlerts storm_reports.json from:
- NWS Local Storm Reports (LSR) via api.weather.gov product endpoints
- SPC preliminary today_torn / today_hail / today_wind CSVs

Output:
  data/storm_reports.json

Notes:
- NWS API requires a unique User-Agent header.
- LSR parsing is a best-effort parser for standard bulletin formatting.
- SPC "today" reports are preliminary and use UTC time.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Iterable

import requests

# -----------------------------
# Config
# -----------------------------

DEFAULT_OUTPUT = Path("data/storm_reports.json")

# Core PA-serving / bordering WFOs that commonly cover PA counties.
DEFAULT_LSR_OFFICES = [
    "CTP",  # State College
    "PBZ",  # Pittsburgh
    "PHI",  # Philadelphia/Mt Holly
    "BGM",  # Binghamton
    "BUF",  # Buffalo
    "CLE",  # Cleveland
]

NWS_API_BASE = "https://api.weather.gov"
SPC_BASE = "https://www.spc.noaa.gov/climo/reports"

# Use something unique here.
DEFAULT_USER_AGENT = "PennAlertsStormBoard/1.0 (contact: you@example.com)"


# -----------------------------
# Models
# -----------------------------

@dataclass
class StormReport:
    id: str
    source: str
    status: str
    office: str
    issued_at: str | None
    event_time: str | None
    county: str | None
    municipality: str | None
    state: str | None
    event_type: str | None
    magnitude: str | None
    description: str | None
    lat: float | None
    lon: float | None
    keywords: list[str]


# -----------------------------
# Helpers
# -----------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def norm_space(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def titleish(s: str | None) -> str | None:
    if not s:
        return None
    return " ".join(part.capitalize() if part.upper() == part else part for part in norm_space(s).split())


def parse_latlon_pair(lat_str: str | None, lon_str: str | None) -> tuple[float | None, float | None]:
    """
    LSR and SPC often use:
      4013 7708 => 40.13, -77.08
      3996 7550 => 39.96, -75.50
    """
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


def clean_keywords(*parts: str | None) -> list[str]:
    text = " ".join(norm_space(p) for p in parts if p)
    if not text:
        return []

    raw = re.split(r"[,;/]| and | with | near | on | at ", text, flags=re.IGNORECASE)
    out: list[str] = []
    seen: set[str] = set()

    for item in raw:
        token = norm_space(item).lower()
        if not token or len(token) < 3:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)

    return out[:12]


def safe_get_json(session: requests.Session, url: str, timeout: int = 30) -> dict[str, Any] | list[Any]:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def safe_get_text(session: requests.Session, url: str, timeout: int = 30) -> str:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def dedupe_reports(items: Iterable[StormReport]) -> list[StormReport]:
    seen: set[str] = set()
    out: list[StormReport] = []

    for r in items:
        key = "|".join([
            norm_space(r.source),
            norm_space(r.office),
            norm_space(r.event_time),
            norm_space(r.county),
            norm_space(r.municipality),
            norm_space(r.event_type),
            norm_space(r.description),
        ]).lower()

        if key in seen:
            continue
        seen.add(key)
        out.append(r)

    return out


def sort_reports(items: list[StormReport]) -> list[StormReport]:
    def keyfunc(r: StormReport):
        ts = r.event_time or r.issued_at or ""
        return ts
    return sorted(items, key=keyfunc, reverse=True)


# -----------------------------
# NWS LSR Fetching
# -----------------------------

def fetch_lsr_product_index(session: requests.Session, office: str) -> list[dict[str, Any]]:
    """
    Tries the common product index endpoint shape for LSR by office.
    """
    url = f"{NWS_API_BASE}/products/types/LSR/locations/{office}"
    data = safe_get_json(session, url)

    if isinstance(data, dict):
        if "@graph" in data and isinstance(data["@graph"], list):
            return data["@graph"]
        if "products" in data and isinstance(data["products"], list):
            return data["products"]
    return []


def extract_product_id(item: dict[str, Any]) -> str | None:
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
    """
    Returns product text and issue time.
    """
    url = f"{NWS_API_BASE}/products/{product_id}"
    data = safe_get_json(session, url)
    text = (
        data.get("productText")
        or data.get("text")
        or data.get("productTextFormatted")
        or ""
    )
    issued = data.get("issuanceTime") or data.get("issued")
    return str(text), issued


# -----------------------------
# LSR Parsing
# -----------------------------

TIME_LINE_RE = re.compile(
    r"^\s*(\d{3,4}\s[AP]M)\s+(.+?)\s{2,}(.+?)\s+(\d{4,5})\s+(\d{4,5})\s*$"
)

DATE_LINE_RE = re.compile(
    r"^\s*(\d{2}/\d{2}/\d{4})\s+(.+?)\s{2,}(.+?)\s+([A-Z]{2})\s+(.+?)\s*$"
)

OFFICE_HEADER_RE = re.compile(r"NATIONAL WEATHER SERVICE\s+([A-Z][A-Z\s\/\.-]+)", re.IGNORECASE)
WMO_TIME_RE = re.compile(r"\b(\d{6})\b")


def parse_lsr_blocks(product_text: str) -> list[list[str]]:
    """
    Best-effort splitter for LSR bulletins into report blocks.
    Looks for lines that begin with a local report time.
    """
    lines = [ln.rstrip() for ln in product_text.splitlines()]

    blocks: list[list[str]] = []
    current: list[str] = []

    in_body = False
    for ln in lines:
        stripped = ln.strip()

        # Start after the standard tabular headers if present
        if stripped.startswith("..TIME...") or stripped.startswith("TIME..."):
            in_body = True
            if current:
                current = []
            continue

        if not in_body:
            continue

        # New block begins
        if TIME_LINE_RE.match(ln):
            if current:
                blocks.append(current)
            current = [ln]
            continue

        if current:
            # Keep non-empty lines; blank line ends nothing because some products are compact
            if stripped:
                current.append(ln)

    if current:
        blocks.append(current)

    return blocks


def parse_lsr_datetime(local_time_text: str, date_text: str) -> datetime | None:
    """
    Example:
      local_time_text: "0640 PM"
      date_text: "03/22/2026"
    Treat as naive local office time if possible; for simplicity,
    we store it as UTC-naive converted? Since office timezone varies,
    this MVP stores the local wall time with UTC tz only if parsed.

    Safer approach for board search:
    keep the date/time ordering consistent even if tz offset is imperfect.
    """
    try:
        dt = datetime.strptime(f"{date_text} {local_time_text}", "%m/%d/%Y %I%M %p")
        # We do not know exact office TZ in this parser; store as UTC-tagged
        # only for consistency in sorting/searching. If desired, later map office->tz.
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_lsr_report_block(block: list[str], office: str, issued_at: str | None, seq: int) -> StormReport | None:
    if len(block) < 2:
        return None

    line1 = block[0]
    line2 = block[1]

    m1 = TIME_LINE_RE.match(line1)
    m2 = DATE_LINE_RE.match(line2)

    if not m1 or not m2:
        return None

    local_time_text = norm_space(m1.group(1))
    event_type = titleish(m1.group(2))
    municipality = titleish(m1.group(3))
    lat_raw = m1.group(4)
    lon_raw = m1.group(5)

    date_text = norm_space(m2.group(1))
    magnitude = norm_space(m2.group(2)) or None
    county = titleish(m2.group(3))
    state = m2.group(4).upper()
    source_line = norm_space(m2.group(5))

    remarks = " ".join(norm_space(x) for x in block[2:]).strip()
    description = remarks or source_line or event_type or "LSR report"

    lat, lon = parse_latlon_pair(lat_raw, lon_raw)
    event_dt = parse_lsr_datetime(local_time_text, date_text)

    rid = f"{date_text.replace('/', '')}-{office.lower()}-{seq:04d}"

    return StormReport(
        id=rid,
        source="NWS_LSR",
        status="preliminary",
        office=office,
        issued_at=issued_at,
        event_time=iso_z(event_dt),
        county=county,
        municipality=municipality,
        state=state,
        event_type=event_type,
        magnitude=magnitude,
        description=description,
        lat=lat,
        lon=lon,
        keywords=clean_keywords(event_type, municipality, county, magnitude, remarks),
    )


def fetch_and_parse_lsrs(
    session: requests.Session,
    offices: list[str],
    max_products_per_office: int = 8,
) -> list[StormReport]:
    reports: list[StormReport] = []

    for office in offices:
        try:
            idx = fetch_lsr_product_index(session, office)
        except Exception as e:
            print(f"[WARN] Failed LSR index for {office}: {e}", file=sys.stderr)
            continue

        product_ids: list[str] = []
        for item in idx[:max_products_per_office]:
            pid = extract_product_id(item)
            if pid:
                product_ids.append(pid)

        seq = 0
        for pid in product_ids:
            try:
                text, issued_at = fetch_product_text(session, pid)
            except Exception as e:
                print(f"[WARN] Failed LSR product {pid} ({office}): {e}", file=sys.stderr)
                continue

            blocks = parse_lsr_blocks(text)
            for block in blocks:
                seq += 1
                report = parse_lsr_report_block(block, office, issued_at, seq)
                if report and (report.state == "PA" or office in {"CTP", "PBZ", "PHI"}):
                    # Keep PA records; bordering offices sometimes include NY/OH/etc.
                    if report.state == "PA":
                        reports.append(report)

    return reports


# -----------------------------
# SPC Fetching
# -----------------------------

def current_spc_storm_day(now: datetime) -> datetime.date:
    """
    SPC 'today' page rolls from 1200 UTC to 1159 UTC next day.
    So the storm day anchor is utc_now - 12 hours.
    """
    return (now - timedelta(hours=12)).date()


def spc_report_datetime_utc(report_time_hhmm: str, storm_day: datetime.date) -> datetime | None:
    digits = re.sub(r"[^\d]", "", report_time_hhmm or "")
    if not digits:
        return None

    try:
        hhmm = digits.zfill(4)
        hh = int(hhmm[:2])
        mm = int(hhmm[2:])
        base = datetime(storm_day.year, storm_day.month, storm_day.day, tzinfo=timezone.utc)

        # Times 1200-2359 belong to storm_day.
        # Times 0000-1159 belong to next UTC day.
        if hh < 12:
            base = base + timedelta(days=1)

        return base.replace(hour=hh, minute=mm, second=0, microsecond=0)
    except Exception:
        return None


def extract_wfo_from_comments(comments: str) -> str | None:
    m = re.search(r"\(([A-Z]{3})\)\s*$", comments.strip())
    if m:
        return m.group(1)
    return None


def parse_spc_csv_text(csv_text: str, kind: str, fetched_at: datetime) -> list[StormReport]:
    """
    Expected columns commonly include:
      Time, Size, Location, County, State, Lat, Lon, Comments
    Wind/hail/tornado vary slightly but this handles the common case.
    """
    rows: list[StormReport] = []
    storm_day = current_spc_storm_day(fetched_at)

    reader = csv.DictReader(StringIO(csv_text))
    seq = 0

    event_type_map = {
        "torn": "Tornado",
        "hail": "Hail",
        "wind": "Thunderstorm Wind Damage",
    }

    for raw in reader:
        state = norm_space(raw.get("State"))
        if state != "PA":
            continue

        seq += 1

        report_time = norm_space(raw.get("Time"))
        magnitude = norm_space(raw.get("Size")) or None
        municipality = titleish(raw.get("Location"))
        county = titleish(raw.get("County"))
        comments = norm_space(raw.get("Comments"))
        office = extract_wfo_from_comments(comments) or "SPC"

        lat_raw = norm_space(raw.get("Lat"))
        lon_raw = norm_space(raw.get("Lon"))
        lat, lon = parse_latlon_pair(lat_raw, lon_raw)

        event_dt = spc_report_datetime_utc(report_time, storm_day)

        description = comments or event_type_map.get(kind, "SPC Report")
        rid = f"{storm_day.strftime('%Y%m%d')}-spc-{kind}-{seq:04d}"

        rows.append(
            StormReport(
                id=rid,
                source="SPC",
                status="preliminary",
                office=office,
                issued_at=iso_z(fetched_at),
                event_time=iso_z(event_dt),
                county=county,
                municipality=municipality,
                state="PA",
                event_type=event_type_map.get(kind, kind.upper()),
                magnitude=magnitude,
                description=description,
                lat=lat,
                lon=lon,
                keywords=clean_keywords(
                    event_type_map.get(kind, kind.upper()),
                    municipality,
                    county,
                    magnitude,
                    comments,
                ),
            )
        )

    return rows


def fetch_spc_today(session: requests.Session, fetched_at: datetime) -> list[StormReport]:
    all_rows: list[StormReport] = []
    for kind in ("torn", "hail", "wind"):
        url = f"{SPC_BASE}/today_{kind}.csv"
        try:
            text = safe_get_text(session, url)
        except Exception as e:
            print(f"[WARN] Failed SPC {kind}: {e}", file=sys.stderr)
            continue
        all_rows.extend(parse_spc_csv_text(text, kind, fetched_at))
    return all_rows


# -----------------------------
# Main
# -----------------------------

def build_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept": "application/ld+json, application/json, text/plain;q=0.9, */*;q=0.8",
    })
    return s


def write_output(path: Path, reports: list[StormReport], fetched_at: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "name": "storm_reports",
        "fetched_at": iso_z(fetched_at),
        "count": len(reports),
        "items": [asdict(r) for r in reports],
    }

    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[OK] Wrote {len(reports)} reports to {path}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build PennAlerts storm_reports.json")
    p.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Output JSON path")
    p.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="Unique User-Agent required by api.weather.gov",
    )
    p.add_argument(
        "--lsr-offices",
        nargs="*",
        default=DEFAULT_LSR_OFFICES,
        help="WFO office IDs to query for LSRs",
    )
    p.add_argument(
        "--max-products-per-office",
        type=int,
        default=8,
        help="How many recent LSR products to inspect per office",
    )
    p.add_argument("--no-lsr", action="store_true", help="Skip NWS LSR ingest")
    p.add_argument("--no-spc", action="store_true", help="Skip SPC ingest")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    fetched_at = now_utc()
    session = build_session(args.user_agent)

    reports: list[StormReport] = []

    if not args.no_lsr:
        lsr_reports = fetch_and_parse_lsrs(
            session=session,
            offices=args.lsr_offices,
            max_products_per_office=args.max_products_per_office,
        )
        print(f"[INFO] Parsed {len(lsr_reports)} LSR reports")
        reports.extend(lsr_reports)

    if not args.no_spc:
        spc_reports = fetch_spc_today(session, fetched_at)
        print(f"[INFO] Parsed {len(spc_reports)} SPC reports")
        reports.extend(spc_reports)

    reports = dedupe_reports(reports)
    reports = sort_reports(reports)
    write_output(args.output, reports, fetched_at)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
