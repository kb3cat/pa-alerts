// power_outages_duq.js
import fs from "fs/promises";

const URL = "https://utilisocial.io/datacapable/v2/p/dlc/map/events";

function getProp(props, key) {
  const found = props.find(p => p.property === key);
  return found ? found.value : [];
}

async function fetchDuquesne() {
  try {
    const res = await fetch(URL);
    const data = await res.json();

    const events = data || [];

    const outages = events.map(e => {
      const props = e.additionalProperties || [];

      const counties = getProp(props, "counties");
      const municipalities = getProp(props, "municipalities");
      const zips = getProp(props, "zips");

      return {
        utility: "Duquesne Light",
        outages: e.numPeople || 0,
        status: e.status || "Unknown",
        county: (counties[0] || "UNKNOWN").toUpperCase(),
        municipality: (municipalities[0] || "UNKNOWN").toUpperCase(),
        zip: zips[0] || "UNKNOWN",
        lat: e.latitude,
        lon: e.longitude
      };
    });

    // ---- totals ----
    const total = outages.reduce((sum, o) => sum + o.outages, 0);

    // ---- county aggregation ----
    const byCounty = {};
    outages.forEach(o => {
      byCounty[o.county] = (byCounty[o.county] || 0) + o.outages;
    });

    // ---- municipality aggregation ----
    const byMunicipality = {};
    outages.forEach(o => {
      byMunicipality[o.municipality] =
        (byMunicipality[o.municipality] || 0) + o.outages;
    });

    const output = {
      name: "power_outages_duq",
      utility: "Duquesne Light",
      fetched_at: new Date().toISOString(),
      total_outages: total,
      counties: byCounty,
      municipalities: byMunicipality,
      raw_count: outages.length,
      items: outages
    };

    await fs.writeFile(
      "./data/power_outages_duq.json",
      JSON.stringify(output, null, 2)
    );

    console.log("Duquesne total outages:", total);

  } catch (err) {
    console.error("Duquesne fetch error:", err);
  }
}

fetchDuquesne();
