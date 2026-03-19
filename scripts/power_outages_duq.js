// scripts/power_outages_duq.js
import fs from "fs/promises";

const EVENTS_URL = "https://utilisocial.io/datacapable/v2/p/dlc/map/events";
const COUNT_URL = "https://utilisocial.io/datacapable/v2/p/dlc/map/count?types=ZIP,COUNTY,MUNICIPALITY";

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

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "accept": "application/json, text/plain, */*",
      "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      "referer": "https://dlc.datacapable.com/",
      "origin": "https://dlc.datacapable.com"
    }
  });

  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }

  return res.json();
}

async function main() {
  try {
    const [eventsData, countData] = await Promise.all([
      fetchJson(EVENTS_URL),
      fetchJson(COUNT_URL)
    ]);

    const events = Array.isArray(eventsData) ? eventsData : [];
    const counts = Array.isArray(countData) ? countData : [];

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

    const counties = {};
    const municipalities = {};

    for (const item of items) {
      counties[item.county] = (counties[item.county] || 0) + toInt(item.outages);
      municipalities[item.municipality] = (municipalities[item.municipality] || 0) + toInt(item.outages);
    }

    const countySummary = [];
    const municipalitySummary = [];
    const zipSummary = [];

    for (const row of counts) {
      const type = String(row.type || "").toUpperCase();
      const name = String(row.name || "").trim();
      const affected = toInt(row.customersAffected);
      const served = toInt(row.customersServed);

      if (!name || !type) continue;

      if (type === "COUNTY") {
        countySummary.push({
          county: normalizeName(name),
          customers_out: affected,
          customers_served: served,
          percent_out: calcPercent(affected, served)
        });
      } else if (type === "MUNICIPALITY") {
        municipalitySummary.push({
          municipality: normalizeName(name),
          customers_out: affected,
          customers_served: served,
          percent_out: calcPercent(affected, served)
        });
      } else if (type === "ZIP") {
        zipSummary.push({
          zip: name,
          customers_out: affected,
          customers_served: served,
          percent_out: calcPercent(affected, served)
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
      counties,
      municipalities,
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
