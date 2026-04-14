from __future__ import annotations

import json
import sys
from dataclasses import dataclass, asdict
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
        # ArcGIS dates are usually Unix epoch milliseconds
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


def fetch_features() -> List[Dict[str, Any]]:
    params = {
        "where": "POOState='PA' AND IncidentTypeCategory='WF'",
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
                "DiscoveryDateTime",
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

    resp = requests.get(WFIGS_URL, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

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
        discovery_time_utc=arcgis_ms_to_iso(props.get("DiscoveryDateTime")),
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
    def sort_key(r: FireRecord):
        # Largest incidents first, then most recently modified
        size = r.incident_size if r.incident_size is not None else -1
        modified = r.modified_time_utc or ""
        return (-size, modified)

    return sorted(records, key=sort_key)


def build_output(records: List[FireRecord]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    largest = max(
        (r.incident_size for r in records if r.incident_size is not None),
        default=None,
    )

    counties = sorted(
        {
            r.county
            for r in records
            if r.county
        }
    )

    return {
        "generated_at_utc": now,
        "source": "WFIGS_Incident_Locations_Current",
        "state_filter": "PA",
        "incident_type_filter": "WF",
        "count": len(records),
        "largest_incident_size_acres": largest,
        "counties_with_incidents": counties,
        "incidents": [asdict(r) for r in records],
    }


def write_json(payload: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> int:
    try:
        features = fetch_features()
        records = [normalize_feature(f) for f in features]

        # Extra safety: keep only PA wildfires even if source/query behavior changes
        records = [
            r for r in records
            if r.state == "PA" and r.incident_type_category == "WF"
        ]

        records = sort_records(records)
        payload = build_output(records)
        write_json(payload, OUT_PATH)

        print(f"Wrote {len(records)} incidents to {OUT_PATH}")
        return 0

    except requests.HTTPError as e:
        print(f"HTTP error fetching WFIGS data: {e}", file=sys.stderr)
        return 1
    except requests.RequestException as e:
        print(f"Network error fetching WFIGS data: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
