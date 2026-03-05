import fs from "node:fs";
import path from "node:path";
import { XMLParser } from "fast-xml-parser";

/**
 * NOTE:
 * - NAS Status endpoint exists at /api/airport-status-information (commonly used for delays/status). :contentReference[oaicite:1]{index=1}
 * - AviationWeather METAR JSON endpoint example: /api/data/metar?ids=KMCI&format=json and includes fltCat. :contentReference[oaicite:2]{index=2}
 */

const OUTPUT_PATH = path.join("data", "airports_live.json");

// Keep this list in sync with your board's airportsPA list (ICAO codes).
const AIRPORTS = [
  "KPIT","KAGC","KLBE","KJST","KERI","KDUJ","KBFD","KBTP","KFKL","KIDI", // Western-ish
  "KMDT","KCXY","KMUI","KUNV","KAOO","KLNS","KIPT",                     // Central-ish
  "KPHL","KABE","KAVP","KHZL"                                            // Eastern-ish
];

const FAA_NAS_XML_URL = "https://nasstatus.faa.gov/api/airport-status-information";
const METAR_URL = (ids) =>
  `https://aviationweather.gov/api/data/metar?ids=${encodeURIComponent(ids.join(","))}&format=json`;

function nowIso() {
  return new Date().toISOString();
}

// Priority: higher wins
const STATUS_PRIORITY = [
  "Closed",
  "Ground Stop",
  "Ground Delay",
  "Arrival Delay",
  "Departure Delay",
  "Deicing",
  "Weather",
  "Other",
  "OK",
  "Unknown"
];

function betterStatus(a, b) {
  const ia = STATUS_PRIORITY.indexOf(a);
  const ib = STATUS_PRIORITY.indexOf(b);
  if (ia === -1 && ib === -1) return a;
  if (ia === -1) return b;
  if (ib === -1) return a;
  return ia <= ib ? a : b;
}

function toArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

function pickFirst(...vals) {
  for (const v of vals) {
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
}

async function fetchText(url) {
  const res = await fetch(url, { headers: { "user-agent": "PA-Airports-Board/1.0 (GitHub Actions)" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

async function fetchJson(url) {
  const res = await fetch(url, { headers: { "user-agent": "PA-Airports-Board/1.0 (GitHub Actions)" } });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.json();
}

/**
 * Heuristic NAS parser:
 * The FAA feed is XML; structure can vary, so we:
 * - Walk delay/closure program lists,
 * - Grab ARPT code wherever it appears,
 * - Pull delay minutes if present,
 * - Map names/reasons to board statuses.
 */
function parseNasXml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    allowBooleanAttributes: true
  });

  const root = parser.parse(xmlText);
  const out = {};
  for (const a of AIRPORTS) out[a] = { status: "OK", delay: "—" };

  // Deep-search helper: find any objects containing ARPT and related fields
  function walk(node, trail = []) {
    if (!node) return;

    if (typeof node === "object") {
      // If this node looks like a "Program" or "Airport status chunk"
      const arpt = pickFirst(node.ARPT, node.Airport, node.airport, node.ARPT_CODE);
      if (arpt && typeof arpt === "string") {
        const code = arpt.trim().toUpperCase();
        if (AIRPORTS.includes(code)) {
          // Try to infer status label from nearby nodes/trail
          const trailStr = trail.join(" > ").toLowerCase();
          const name = String(pickFirst(node.Name, node.TYPE, node.Type, node.Event, node.Reason, "")).toLowerCase();

          let status = "Other";
          if (trailStr.includes("closure") || name.includes("closure") || name.includes("closed")) status = "Closed";
          else if (trailStr.includes("ground_stop") || trailStr.includes("ground stop") || name.includes("ground stop")) status = "Ground Stop";
          else if (trailStr.includes("ground_delay") || trailStr.includes("gdp") || name.includes("ground delay")) status = "Ground Delay";
          else if (trailStr.includes("arrival") || name.includes("arrival")) status = "Arrival Delay";
          else if (trailStr.includes("departure") || name.includes("departure")) status = "Departure Delay";
          else if (trailStr.includes("deicing") || name.includes("deicing")) status = "Deicing";

          // Delay minutes: different feeds use different field names
          const delayVal = pickFirst(
            node.AvgDelay,
            node.AVG_DELAY,
            node.Delay,
            node.DELAY,
            node.Delay_Minutes,
            node.Minutes
          );

          // Normalize delay
          let delay = "—";
          if (delayVal !== null && delayVal !== undefined) {
            const n = Number(String(delayVal).replace(/[^\d.]/g, ""));
            if (Number.isFinite(n) && n > 0) delay = String(Math.round(n));
          }

          // Apply
          out[code].status = betterStatus(out[code].status, status);
          if (delay !== "—") out[code].delay = delay;
        }
      }

      for (const [k, v] of Object.entries(node)) {
        if (k.startsWith("@_")) continue;
        const nextTrail = [...trail, k];
        if (Array.isArray(v)) v.forEach((x) => walk(x, nextTrail));
        else if (typeof v === "object") walk(v, nextTrail);
      }
    }
  }

  walk(root, []);
  return out;
}

function parseMetars(metarJson) {
  // metarJson is an array of objects
  const out = {};
  for (const a of AIRPORTS) out[a] = { cat: "—", raw: "—", obsTime: null };

  for (const m of toArray(metarJson)) {
    const station = String(pickFirst(m.icaoId, m.station, m.stationId, m.id, m.icao, m.rawOb?.slice(0, 4), "") || "").toUpperCase();
    if (!AIRPORTS.includes(station)) continue;

    const cat = pickFirst(m.fltCat, m.flightCategory, m.fltcat) || "—";
    const raw = pickFirst(m.rawOb, m.raw, m.metar, m.text) || "—";
    const obsTime = pickFirst(m.obsTime, m.observationTime, m.reportTime) || null;

    out[station] = { cat, raw, obsTime };
  }
  return out;
}

async function main() {
  // 1) FAA NAS (XML)
  let nasMap;
  try {
    const nasXml = await fetchText(FAA_NAS_XML_URL);
    nasMap = parseNasXml(nasXml);
  } catch (e) {
    nasMap = {};
    console.error("NAS fetch/parse failed:", e.message);
  }

  // 2) METAR JSON
  let metarMap;
  try {
    const metars = await fetchJson(METAR_URL(AIRPORTS));
    metarMap = parseMetars(metars);
  } catch (e) {
    metarMap = {};
    console.error("METAR fetch/parse failed:", e.message);
  }

  // 3) Combine
  const combined = {
    generatedAt: nowIso(),
    airports: {}
  };

  for (const icao of AIRPORTS) {
    const nas = nasMap?.[icao] || { status: "Unknown", delay: "—" };
    const met = metarMap?.[icao] || { cat: "—", raw: "—", obsTime: null };

    combined.airports[icao] = {
      status: nas.status ?? "Unknown",
      delay: nas.delay ?? "—",
      metarCat: met.cat ?? "—",
      metarRaw: met.raw ?? "—",
      metarObsTime: met.obsTime ?? null
    };
  }

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(combined, null, 2));
  console.log(`Wrote ${OUTPUT_PATH} at ${combined.generatedAt}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
