#!/usr/bin/env python3

import json
from datetime import datetime, timezone
from pathlib import Path

import requests

URL = "https://rhvpkkiftonktxq3.svcs9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services/USA_Wildfires_v1/FeatureServer/0/query"

OUT = Path("data/pa_dcnr_fires.json")


def fetch():
    params = {
        "where": "1=1",  # we filter PA locally
        "outFields": "*",
        "returnGeometry": "true",
        "f": "geojson"
    }

    r = requests.get(URL, params=params, timeout=45)
    r.raise_for_status()
    return r.json()["features"]


def is_pa(record):
    props = record["properties"]

    # try multiple possible fields
    return (
        props.get("State") == "PA"
        or props.get("POOState") == "PA"
        or props.get("STATE") == "PA"
    )


def normalize(feature):
    p = feature["properties"]
    g = feature["geometry"]["coordinates"]

    return {
        "name": p.get("IncidentName") or p.get("IRWINID") or "Unnamed",
        "acres": p.get("DailyAcres") or p.get("IncidentSize"),
        "contained": p.get("PercentContained"),
        "discovered": p.get("FireDiscoveryDateTime"),
        "category": p.get("FeatureCategory"),
        "county": p.get("POOCounty") or p.get("County"),
        "lat": g[1],
        "lon": g[0],
        "raw": p  # keep everything for debugging
    }


def main():
    features = fetch()

    pa = [f for f in features if is_pa(f)]
    records = [normalize(f) for f in pa]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(records),
        "fires": records
    }

    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2))

    print(f"PA fires: {len(records)}")


if __name__ == "__main__":
    main()
