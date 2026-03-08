#!/usr/bin/env python3

import shutil
import zipfile
from pathlib import Path

import geopandas as gpd
import requests

# Official sources
CWA_URL = "https://www.weather.gov/source/gis/Shapefiles/WSOM/w_16ap26.zip"
STATE_URL = "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_state_500k.zip"

WORKDIR = Path("debug/boundary_build")
OUTDIR = Path("data")

TARGET_OFFICES = {"PBZ", "CLE", "CTP", "BGM", "PHI"}


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    dest.write_bytes(r.content)


def unzip(zip_path: Path, dest_dir: Path) -> None:
    if dest_dir.exists():
        shutil.rmtree(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)


def find_shp(directory: Path) -> Path:
    shp_files = list(directory.rglob("*.shp"))
    if not shp_files:
        raise FileNotFoundError(f"No .shp file found in {directory}")
    return shp_files[0]


def simplify_geojson(path: Path, tolerance: float = 0.01) -> None:
    gdf = gpd.read_file(path)
    gdf["geometry"] = gdf.geometry.simplify(tolerance=tolerance, preserve_topology=True)
    gdf.to_file(path, driver="GeoJSON")


def main() -> None:
    WORKDIR.mkdir(parents=True, exist_ok=True)
    OUTDIR.mkdir(parents=True, exist_ok=True)

    cwa_zip = WORKDIR / "cwa.zip"
    cwa_dir = WORKDIR / "cwa"
    state_zip = WORKDIR / "state.zip"
    state_dir = WORKDIR / "state"

    print("Downloading official CWA boundaries...")
    download(CWA_URL, cwa_zip)
    unzip(cwa_zip, cwa_dir)

    print("Downloading Census state boundaries...")
    download(STATE_URL, state_zip)
    unzip(state_zip, state_dir)

    cwa_shp = find_shp(cwa_dir)
    state_shp = find_shp(state_dir)

    print("Reading shapefiles...")
    cwa_gdf = gpd.read_file(cwa_shp)
    state_gdf = gpd.read_file(state_shp)

    if cwa_gdf.crs is None or str(cwa_gdf.crs).lower() != "epsg:4326":
        cwa_gdf = cwa_gdf.to_crs(epsg=4326)
    if state_gdf.crs is None or str(state_gdf.crs).lower() != "epsg:4326":
        state_gdf = state_gdf.to_crs(epsg=4326)

    # NWS field is commonly CWA
    if "CWA" not in cwa_gdf.columns:
        raise KeyError(f"Could not find CWA column in {list(cwa_gdf.columns)}")

    cwa_filtered = cwa_gdf[cwa_gdf["CWA"].isin(TARGET_OFFICES)].copy()
    cwa_filtered = cwa_filtered[["CWA", "geometry"]].rename(columns={"CWA": "office"})

    # Census field for state abbreviation
    if "STUSPS" not in state_gdf.columns:
        raise KeyError(f"Could not find STUSPS column in {list(state_gdf.columns)}")

    pa = state_gdf[state_gdf["STUSPS"] == "PA"].copy()
    pa = pa[["STUSPS", "geometry"]].rename(columns={"STUSPS": "state"})

    cwa_out = OUTDIR / "cwa_boundaries.geojson"
    pa_out = OUTDIR / "pa_boundary.geojson"

    print(f"Writing {cwa_out} ...")
    cwa_filtered.to_file(cwa_out, driver="GeoJSON")

    print(f"Writing {pa_out} ...")
    pa.to_file(pa_out, driver="GeoJSON")

    print("Simplifying GeoJSON for web map use...")
    simplify_geojson(cwa_out, tolerance=0.01)
    simplify_geojson(pa_out, tolerance=0.005)

    print("Done.")


if __name__ == "__main__":
    main()
