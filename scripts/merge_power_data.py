#!/usr/bin/env python3

import json
from datetime import datetime, timezone
from pathlib import Path


FE_FILE = Path("data/power_outages_pa.json")
PPL_FILE = Path("data/power_outages_ppl.json")
DUQ_FILE = Path("data/power_outages_duq.json")
PECO_FILE = Path("data/power_outages_peco.json")
OUTPUT_FILE = Path("data/power_outages_combined.json")


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def load_json(path: Path):
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def to_int(value):
    try:
        if value is None:
            return 0
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return 0


def normalize_county_name(name: str) -> str:
    return str(name or "").strip()


def merge_counties(fe_data, ppl_data, duq_data, peco_data):
    merged = {}

    # ---- FirstEnergy ----
    fe_counties = fe_data.get("county_summary", [])
    for row in fe_counties:
        county = normalize_county_name(row.get("county"))
        if not county:
            continue

        merged[county] = {
            "county": county,
            "customers_out": to_int(row.get("customers_out")),
            "customers_served": to_int(row.get("customers_served")),
            "change": to_int(row.get("change")),
            "sources": ["FirstEnergy"],
            "firstenergy": {
                "customers_out": to_int(row.get("customers_out")),
                "customers_served": to_int(row.get("customers_served")),
                "percent_out": row.get("percent_out"),
                "change": to_int(row.get("change")),
            },
            "ppl": None,
            "duquesne": None,
            "peco": None,
        }

    # ---- PPL ----
    ppl_counties = ppl_data.get("county_summary", [])
    for row in ppl_counties:
        county = normalize_county_name(row.get("county"))
        if not county:
            continue

        ppl_out = to_int(row.get("customers_out"))
        ppl_served = to_int(row.get("customers_served"))

        if county not in merged:
            merged[county] = {
                "county": county,
                "customers_out": 0,
                "customers_served": 0,
                "change": 0,
                "sources": [],
                "firstenergy": None,
                "ppl": None,
                "duquesne": None,
                "peco": None,
            }

        merged[county]["customers_out"] += ppl_out
        merged[county]["customers_served"] += ppl_served

        if "PPL" not in merged[county]["sources"]:
            merged[county]["sources"].append("PPL")

        merged[county]["ppl"] = {
            "customers_out": ppl_out,
            "customers_served": ppl_served,
            "percent_out": row.get("percent_out"),
        }

    # ---- Duquesne ----
    duq_counties = duq_data.get("county_summary", [])
    for row in duq_counties:
        county = normalize_county_name(row.get("county"))
        if not county:
            continue

        duq_out = to_int(row.get("customers_out"))
        duq_served = to_int(row.get("customers_served"))

        if county not in merged:
            merged[county] = {
                "county": county,
                "customers_out": 0,
                "customers_served": 0,
                "change": 0,
                "sources": [],
                "firstenergy": None,
                "ppl": None,
                "duquesne": None,
                "peco": None,
            }

        merged[county]["customers_out"] += duq_out
        merged[county]["customers_served"] += duq_served

        if "Duquesne Light" not in merged[county]["sources"]:
            merged[county]["sources"].append("Duquesne Light")

        merged[county]["duquesne"] = {
            "customers_out": duq_out,
            "customers_served": duq_served,
            "percent_out": row.get("percent_out"),
        }

    # ---- PECO ----
    peco_counties = peco_data.get("counties", [])
    for row in peco_counties:
        county = normalize_county_name(row.get("name") or row.get("county"))
        if not county:
            continue

        peco_out = to_int(row.get("customers_affected") or row.get("customers_out"))
        peco_served = to_int(row.get("customers_served"))
        peco_percent = row.get("percent_affected")
        peco_outages = to_int(row.get("outages"))

        if county not in merged:
            merged[county] = {
                "county": county,
                "customers_out": 0,
                "customers_served": 0,
                "change": 0,
                "sources": [],
                "firstenergy": None,
                "ppl": None,
                "duquesne": None,
                "peco": None,
            }

        merged[county]["customers_out"] += peco_out
        merged[county]["customers_served"] += peco_served

        if "PECO" not in merged[county]["sources"]:
            merged[county]["sources"].append("PECO")

        merged[county]["peco"] = {
            "customers_out": peco_out,
            "customers_served": peco_served,
            "percent_out": peco_percent,
            "outages": peco_outages,
            "etr": row.get("etr"),
            "masked": row.get("masked"),
        }

    county_rows = []
    for county, row in merged.items():
        served = to_int(row.get("customers_served"))
        out = to_int(row.get("customers_out"))
        percent = round((out / served) * 100, 2) if served > 0 else None

        county_rows.append({
            "county": county,
            "customers_out": out,
            "customers_served": served,
            "percent_out": percent,
            "change": to_int(row.get("change")),
            "sources": row.get("sources", []),
            "firstenergy": row.get("firstenergy"),
            "ppl": row.get("ppl"),
            "duquesne": row.get("duquesne"),
            "peco": row.get("peco"),
        })

    county_rows.sort(
        key=lambda x: (
            -(x["customers_out"] or 0),
            x["county"].lower()
        )
    )

    return county_rows


def build_summary(fe_data, county_rows):
    total_customers_out = sum(to_int(r.get("customers_out")) for r in county_rows)
    total_customers_served = sum(to_int(r.get("customers_served")) for r in county_rows)
    total_percent_out = round((total_customers_out / total_customers_served) * 100, 2) if total_customers_served else None

    counties_over_one = sum(
        1 for r in county_rows
        if r.get("percent_out") is not None and r.get("percent_out") > 1
    )

    fe_summary = fe_data.get("summary", {})

    return {
        "new": to_int(fe_summary.get("new")),
        "increasing": to_int(fe_summary.get("increasing")),
        "decreasing": to_int(fe_summary.get("decreasing")),
        "unchanged": to_int(fe_summary.get("unchanged")),
        "restored": to_int(fe_summary.get("restored")),
        "counties_over_one_percent": counties_over_one,
        "total_customers_served": total_customers_served,
        "total_percent_out": total_percent_out,
    }, total_customers_out


def build_utility_summary(fe_data, ppl_data, duq_data, peco_data):
    rows = []

    fe_total = to_int(fe_data.get("total_customers_out") or fe_data.get("total_outages"))
    ppl_total = to_int(ppl_data.get("total_customers_out") or ppl_data.get("total_outages"))
    duq_total = to_int(duq_data.get("total_outages"))
    peco_utility = peco_data.get("utility", {})
    peco_total = to_int(peco_utility.get("customers_affected") or peco_utility.get("customers_out"))
    peco_served = to_int(peco_utility.get("customers_served"))
    peco_outages = to_int(peco_utility.get("outages"))
    peco_percent = peco_utility.get("percent_affected")

    if fe_total > 0:
        rows.append({
            "utility": "FirstEnergy",
            "customers_out": fe_total,
        })

    if ppl_total > 0:
        rows.append({
            "utility": "PPL",
            "customers_out": ppl_total,
        })

    if duq_total > 0:
        rows.append({
            "utility": "Duquesne Light",
            "customers_out": duq_total,
        })

    if peco_total > 0:
        rows.append({
            "utility": "PECO",
            "customers_out": peco_total,
            "customers_served": peco_served,
            "percent_out": peco_percent,
            "outages": peco_outages,
        })

    rows.sort(key=lambda x: (-to_int(x["customers_out"]), x["utility"]))
    return rows


def build_municipality_summary(duq_data, peco_data):
    rows = []

    for row in duq_data.get("municipality_summary", []):
        item = dict(row)
        item.setdefault("source", "Duquesne Light")
        rows.append(item)

    for row in peco_data.get("municipalities", []):
        rows.append({
            "municipality": row.get("name"),
            "county": row.get("county"),
            "customers_out": to_int(row.get("customers_affected") or row.get("customers_out")),
            "customers_served": to_int(row.get("customers_served")),
            "percent_out": row.get("percent_affected"),
            "outages": to_int(row.get("outages")),
            "etr": row.get("etr"),
            "masked": row.get("masked"),
            "source": "PECO",
        })

    rows = sorted(
        rows,
        key=lambda x: (-to_int(x.get("customers_out")), x.get("municipality", "").lower())
    )
    return rows


def main():
    fe_data = load_json(FE_FILE)
    ppl_data = load_json(PPL_FILE)
    duq_data = load_json(DUQ_FILE)
    peco_data = load_json(PECO_FILE)

    county_rows = merge_counties(fe_data, ppl_data, duq_data, peco_data)
    summary, total_customers_out = build_summary(fe_data, county_rows)
    utility_summary = build_utility_summary(fe_data, ppl_data, duq_data, peco_data)
    municipality_summary = build_municipality_summary(duq_data, peco_data)

    output = {
        "name": "pa_power_outages_combined",
        "fetched_at": iso_utc_now(),
        "sources": {
            "firstenergy_file": str(FE_FILE),
            "ppl_file": str(PPL_FILE),
            "duquesne_file": str(DUQ_FILE),
            "peco_file": str(PECO_FILE),
            "firstenergy_fetched_at": fe_data.get("fetched_at"),
            "ppl_fetched_at": ppl_data.get("fetched_at"),
            "duquesne_fetched_at": duq_data.get("fetched_at"),
            "peco_fetched_at": peco_data.get("fetched_at"),
            "ppl_source_last_updated": ppl_data.get("source_last_updated"),
            "peco_source_date_generated": peco_data.get("utility", {}).get("date_generated"),
        },
        "total_customers_out": total_customers_out,
        "summary": summary,
        "utility_summary": utility_summary,
        "county_summary": county_rows,
        "municipality_summary": municipality_summary,
        "items_firstenergy": fe_data.get("items", []),
        "items_ppl": ppl_data.get("items", []),
        "items_duquesne": duq_data.get("items", []),
        "items_peco": peco_data.get("municipalities", []),
    }

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {OUTPUT_FILE}")
    print(f"County rows: {len(county_rows)}")
    print(f"Utility rows: {len(utility_summary)}")
    print(f"Municipality rows: {len(municipality_summary)}")
    print(f"Total customers out: {total_customers_out}")


if __name__ == "__main__":
    main()
