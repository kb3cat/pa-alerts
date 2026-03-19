// scripts/power_outages_duq.js
import fs from "fs/promises";

const EVENTS_URL = "https://utilisocial.io/datacapable/v2/p/dlc/map/events";
const COUNTS_URL = "https://utilisocial.io/datacapable/v2/p/dlc/map/count?types=ZIP,COUNTY,MUNICIPALITY";

function getProp(props, key) {
  const found = (props || []).find((p) => p.property === key);
  return found ? found.value : [];
}

function toInt(value) {
  if (value === null || value === undefined || value === "") return 0;
  const n = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function normalizeName(value) {
  return String(value || "").trim().toUpperCase();
}

function calcPercent(out, served) {
  return served > 0 ? Number(((out / served) * 100).toFixed(2)) : null;
}

function extractGroupedCounts(payload) {
  // Supports a few possible response shapes.
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.results)) return payload.results;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.items)) return payload.items;
  return [];
}

function pickGroupType(row) {
  return String(
    row?.type ??
    row?.groupType ??
    row?.category ??
    row?.name ??
    ""
  ).toUpperCase();
}

function pickLabel(row) {
  return String(
    row?.label ??
    row?.key ??
    row?.value ??
    row?.name ??
    row?.title ??
    row?.group ??
    ""
  ).trim();
}

function pickCustomersOut(row) {
  return toInt(
    row?.customersOut ??
    row?.customers_out ??
    row?.numPeople ??
    row?.out ??
    row?.count
  );
}

function pickCustomersServed(row) {
  return toInt(
    row?.customersServed ??
    row?.customers_served ??
    row?.served
  );
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "accept": "application/json, text/plain, */*",
      "user-agent": "PennAlerts Power Outages Duquesne Script"
    }
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }

  return res.json();
}

async function main() {
  try {
    const [eventsData, countsData] = await Promise.all([
      fetchJson(EVENTS_URL),
      fetchJson(COUNTS_URL)
    ]);

    const events = Array.isArray(eventsData) ? eventsData : [];

    const items = events.map((e) => {
      const props = e.additionalProperties || [];
      const counties = getProp(props, "counties");
      const municipalities = getProp(props, "municipalities");
      const zips = getProp(props, "zips");

      return {
        utility: "Duquesne Light",
        outages: toInt(e.numPeople),
        status: e.status || "Unknown",
        county: normalizeName(counties[0] || "UNKNOWN"),
        municipality: normalizeName(municipalities[0] || "UNKNOWN"),
        zip: String(zips[0] || "UNKNOWN").trim(),
        lat: typeof e.latitude === "number" ? e.latitude : null,
        lon: typeof e.longitude === "number" ? e.longitude : null
      };
    });

    const totalOutages = items.reduce((sum, item) => sum + toInt(item.outages), 0);

    // Municipalities from events
    const municipalitiesAgg = {};
    for (const item of items) {
      const muni = item.municipality || "UNKNOWN";
      municipalitiesAgg[muni] = (municipalitiesAgg[muni] || 0) + toInt(item.outages);
    }

    // Parse grouped counts
    const grouped = extractGroupedCounts(countsData);

    const countySummary = [];
    const countiesAgg = {};
    const zipSummary = [];
    const municipalitySummary = [];

    for (const row of grouped) {
      const type = pickGroupType(row);
      const label = normalizeName(pickLabel(row));
      const out = pickCustomersOut(row);
      const served = pickCustomersServed(row);

      if (!label) continue;

      if (type === "COUNTY") {
        countySummary.push({
          county: label,
          customers_out: out,
          customers_served: served,
          percent_out: calcPercent(out, served)
        });
        countiesAgg[label] = out;
      } else if (type === "ZIP") {
        zipSummary.push({
          zip: String(pickLabel(row)).trim(),
          customers_out: out,
          customers_served: served,
          percent_out: calcPercent(out, served)
        });
      } else if (type === "MUNICIPALITY") {
        municipalitySummary.push({
          municipality: label,
          customers_out: out,
          customers_served: served,
          percent_out: calcPercent(out, served)
        });
      }
    }

    // Fallback if grouped municipality counts are absent
    if (municipalitySummary.length === 0) {
      for (const [municipality, outages] of Object.entries(municipalitiesAgg)) {
        municipalitySummary.push({
          municipality,
          customers_out: outages,
          customers_served: null,
          percent_out: null
        });
      }
    }

    countySummary.sort((a, b) => {
      const diff = toInt(b.customers_out) - toInt(a.customers_out);
      if (diff !== 0) return diff;
      return a.county.localeCompare(b.county);
    });

    municipalitySummary.sort((a, b) => {
      const diff = toInt(b.customers_out) - toInt(a.customers_out);
      if (diff !== 0) return diff;
      return a.municipality.localeCompare(b.municipality);
    });

    zipSummary.sort((a, b) => {
      const diff = toInt(b.customers_out) - toInt(a.customers_out);
      if (diff !== 0) return diff;
      return String(a.zip).localeCompare(String(b.zip));
    });

    const output = {
      name: "power_outages_duq",
      utility: "Duquesne Light",
      fetched_at: new Date().toISOString(),
      total_outages: totalOutages,
      counties: countiesAgg,
      municipalities: municipalitiesAgg,
      county_summary: countySummary,
      municipality_summary: municipalitySummary,
      zip_summary: zipSummary,
      raw_count: items.length,
      items
    };

    await fs.mkdir("./data", { recursive: true });
    await fs.writeFile(
      "./data/power_outages_duq.json",
      JSON.stringify(output, null, 2),
      "utf8"
    );

    console.log("Wrote ./data/power_outages_duq.json");
    console.log(`Duquesne total outages: ${totalOutages}`);
    console.log(`County rows: ${countySummary.length}`);
    console.log(`Municipality rows: ${municipalitySummary.length}`);
    console.log(`ZIP rows: ${zipSummary.length}`);
  } catch (err) {
    console.error("Duquesne fetch error:", err);
    process.exit(1);
  }
}

main();
