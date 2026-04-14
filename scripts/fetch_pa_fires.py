#!/usr/bin/env python3
"""
fetch_pa_fires.py

Pull current Pennsylvania incident records from the public WFIGS ArcGIS layer
and write a normalized JSON file for a dashboard.

Output:
  data/pa_fires.json
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


WFIGS_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "WFIGS_Incident_Locations_Current/FeatureServer/0/query"
)

OUT_PATH = Path("data/pa_fires.json")
TIMEOUT = 45


@dataclass
class FireRecord:
    incident_name: str
    irwin_id: Optional[str]
    incident_type_category: Optional[str]
    incident_type_kind: Optional[str]
    state: Optional[str]
    county: Optional[str]
    city: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    initial_latitude: Optional[float]
    initial_longitude: Optional[float]
    incident_size: Optional[float]
    percent_contained: Optional[float]
    discovery_time_utc: Optional[str]
    initial_response_time_utc: Optional[str]
    modified_time_utc: Optional[str]
    created_time_utc: Optional[str]
    short_description: Optional[str]
    jurisdictional_agency: Optional[str]
    protecting_agency: Optional[str]
    dispatch_center_id: Optional[str]
    active_fire_candidate: Optional[int]
    is_complex_child: Optional[int]
    complex_name: Optional[str]
    data_source: str = "WFIGS_Incident_Locations_Current"


def arcgis_ms_to_iso(value: Any) -> Optional[str]:
    if value in (None, "", 0):
        return None
    try:
        dt = datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def clean_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def clean_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def clean_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def run_count(where: str) -> int:
    params = {
        "where": where,
        "returnCountOnly": "true",
        "f": "json",
    }
    response = requests.get(WFIGS_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    return int(payload.get("count", 0))


def fetch_features() -> List[Dict[str, Any]]:
    params = {
        "where": "POOState='PA'",
        "outFields": ",".join(
            [
                "IncidentName",
                "IrwinID",
                "IncidentTypeCategory",
                "IncidentTypeKind",
                "POOState",
                "POOCounty",
                "POOCity",
                "IncidentSize",
                "PercentContained",
                "FireDiscoveryDateTime",
                "InitialResponseDateTime",
                "ModifiedOnDateTime_dt",
                "CreatedOnDateTime_dt",
                "IncidentShortDescription",
                "POOJurisdictionalAgency",
                "POOProtectingAgency",
                "POODispatchCenterID",
                "InitialLatitude",
                "InitialLongitude",
                "ActiveFireCandidate",
                "IsCpxChild",
                "CpxName",
            ]
        ),
        "returnGeometry": "true",
        "f": "geojson",
    }

    response = requests.get(WFIGS_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    data = response.json()

    features = data.get("features", [])
    if not isinstance(features, list):
        raise ValueError("Unexpected response: missing feature list")

    return features


def normalize_feature(feature: Dict[str, Any]) -> FireRecord:
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry", {}) or {}

    coords = geom.get("coordinates") or [None, None]
    longitude = clean_float(coords[0]) if len(coords) > 0 else None
    latitude = clean_float(coords[1]) if len(coords) > 1 else None

    return FireRecord(
        incident_name=clean_str(props.get("IncidentName")) or "Unnamed Incident",
        irwin_id=clean_str(props.get("IrwinID")),
        incident_type_category=clean_str(props.get("IncidentTypeCategory")),
        incident_type_kind=clean_str(props.get("IncidentTypeKind")),
        state=clean_str(props.get("POOState")),
        county=clean_str(props.get("POOCounty")),
        city=clean_str(props.get("POOCity")),
        latitude=latitude,
        longitude=longitude,
        initial_latitude=clean_float(props.get("InitialLatitude")),
        initial_longitude=clean_float(props.get("InitialLongitude")),
        incident_size=clean_float(props.get("IncidentSize")),
        percent_contained=clean_float(props.get("PercentContained")),
        discovery_time_utc=arcgis_ms_to_iso(props.get("FireDiscoveryDateTime")),
        initial_response_time_utc=arcgis_ms_to_iso(props.get("InitialResponseDateTime")),
        modified_time_utc=arcgis_ms_to_iso(props.get("ModifiedOnDateTime_dt")),
        created_time_utc=arcgis_ms_to_iso(props.get("CreatedOnDateTime_dt")),
        short_description=clean_str(props.get("IncidentShortDescription")),
        jurisdictional_agency=clean_str(props.get("POOJurisdictionalAgency")),
        protecting_agency=clean_str(props.get("POOProtectingAgency")),
        dispatch_center_id=clean_str(props.get("POODispatchCenterID")),
        active_fire_candidate=clean_int(props.get("ActiveFireCandidate")),
        is_complex_child=clean_int(props.get("IsCpxChild")),
        complex_name=clean_str(props.get("CpxName")),
    )


def sort_records(records: List[FireRecord]) -> List[FireRecord]:
    def sort_key(record: FireRecord):
        size = record.incident_size if record.incident_size is not None else -1
        modified = record.modified_time_utc or ""
        name = record.incident_name or ""
        return (-size, modified, name)

    return sorted(records, key=sort_key, reverse=False)


def summarize_bucket(records: List[FireRecord]) -> Dict[str, Any]:
    largest = max(
        (r.incident_size for r in records if r.incident_size is not None),
        default=None,
    )
    counties = sorted({r.county for r in records if r.county})
    return {
        "count": len(records),
        "largest_incident_size_acres": largest,
        "counties": counties,
    }


def build_output(
    all_records: List[FireRecord],
    confirmed_wildfires: List[FireRecord],
    active_fire_candidates: List[FireRecord],
    other_records: List[FireRecord],
    debug_counts: Dict[str, int],
) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    categories = Counter(r.incident_type_category or "UNKNOWN" for r in all_records)

    return {
        "generated_at_utc": now,
        "source": "WFIGS_Incident_Locations_Current",
        "state_filter": "PA",
        "debug_counts": debug_counts,
        "category_breakdown": dict(categories),
        "summary": {
            "all_pa_records": summarize_bucket(all_records),
            "confirmed_wildfires": summarize_bucket(confirmed_wildfires),
            "active_fire_candidates": summarize_bucket(active_fire_candidates),
            "other_records": summarize_bucket(other_records),
        },
        "confirmed_wildfires": [asdict(r) for r in confirmed_wildfires],
        "active_fire_candidates": [asdict(r) for r in active_fire_candidates],
        "other_records": [asdict(r) for r in other_records],
    }


def write_json(payload: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    try:
        debug_counts = {
            "pa_all_incidents": run_count("POOState='PA'"),
            "pa_wildfires": run_count("POOState='PA' AND IncidentTypeCategory='WF'"),
            "pa_active_fire_candidates": run_count("POOState='PA' AND ActiveFireCandidate=1"),
        }

        features = fetch_features()
        all_records = [normalize_feature(feature) for feature in features]
        all_records = [record for record in all_records if record.state == "PA"]

        confirmed_wildfires = [
            record for record in all_records
            if record.incident_type_category == "WF"
        ]

        active_fire_candidates = [
            record for record in all_records
            if record.active_fire_candidate == 1
        ]

        confirmed_ids = {
            (r.irwin_id or "", r.incident_name, r.modified_time_utc or "")
            for r in confirmed_wildfires
        }
        candidate_ids = {
            (r.irwin_id or "", r.incident_name, r.modified_time_utc or "")
            for r in active_fire_candidates
        }

        other_records = [
            record for record in all_records
            if (record.irwin_id or "", record.incident_name, record.modified_time_utc or "")
            not in confirmed_ids
            and (record.irwin_id or "", record.incident_name, record.modified_time_utc or "")
            not in candidate_ids
        ]

        all_records = sort_records(all_records)
        confirmed_wildfires = sort_records(confirmed_wildfires)
        active_fire_candidates = sort_records(active_fire_candidates)
        other_records = sort_records(other_records)

        payload = build_output(
            all_records=all_records,
            confirmed_wildfires=confirmed_wildfires,
            active_fire_candidates=active_fire_candidates,
            other_records=other_records,
            debug_counts=debug_counts,
        )

        write_json(payload, OUT_PATH)

        print(f"Wrote {len(all_records)} PA records to {OUT_PATH}")
        print(f"Confirmed wildfires: {len(confirmed_wildfires)}")
        print(f"Active fire candidates: {len(active_fire_candidates)}")
        print(f"Other records: {len(other_records)}")
        print(f"Category breakdown: {payload['category_breakdown']}")

        return 0

    except requests.HTTPError as exc:
        print(f"HTTP error fetching WFIGS data: {exc}", file=sys.stderr)
        return 1
    except requests.RequestException as exc:
        print(f"Network error fetching WFIGS data: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
