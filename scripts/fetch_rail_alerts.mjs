// scripts/fetch_rail_alerts.mjs
import fs from "fs/promises";

const OUT = "data/rail_alerts.json";

// Fallback “centroids” for map markers (Phase 1 only; refine later)
const CENTROIDS = {
  // SEPTA RR (rough centers)
  "Airport Line": [39.90, -75.20],
  "Chestnut Hill East Line": [40.06, -75.17],
  "Chestnut Hill West Line": [40.05, -75.21],
  "Cynwyd Line": [39.99, -75.25],
  "Fox Chase Line": [40.05, -75.09],
  "Lansdale/Doylestown Line": [40.29, -75.13],
  "Manayunk/Norristown Line": [40.08, -75.30],
  "Media/Wawa Line": [39.93, -75.38],
  "Paoli/Thorndale Line": [40.04, -75.52],
  "Trenton Line": [40.10, -74.86],
  "Warminster Line": [40.20, -75.08],
  "Wilmington/Newark Line": [39.88, -75.44],
  "West Trenton Line": [40.26, -74.99],

  // Amtrak (PA corridor approximations)
  "Keystone Service": [40.08, -76.30],      // Lancaster-ish midpoint
  "Pennsylvanian": [40.44, -79.99],         // Pittsburgh-ish fallback
  "Northeast Regional": [39.95, -75.16],    // Philly fallback
  "Acela": [39.95, -75.16]
};

function pickCentroid(locationText = "") {
  for (const [k, v] of Object.entries(CENTROIDS)) {
    if (locationText.toLowerCase().includes(k.toLowerCase())) return v;
  }
  return null;
}

function extractETR(text = "") {
  // Phase 1: best-effort parse (often not present)
  // Looks for things like "ETR 10:30", "estimated time to restore 6 PM", "expected to resume at"
  const t = text.replace(/\s+/g, " ").trim();

  const m1 = t.match(/\bETR[:\s]*([0-9]{1,2}:[0-9]{2}\s*(AM|PM)?|\b[0-9]{1,2}\s*(AM|PM)\b)/i);
  if (m1) return m1[1].toUpperCase();

  const m2 = t.match(/\b(expected to resume|service to resume|resume service|restored)\s+(at|by)\s+([0-9]{1,2}(:[0-9]{2})?\s*(AM|PM)?)/i);
  if (m2) return m2[3].toUpperCase();

  return "";
}

async function fetchJson(url) {
  const r = await fetch(url, { headers: { "User-Agent": "pa-rail-board/1.0" } });
  if (!r.ok) throw new Error(`${url} -> HTTP ${r.status}`);
  return r.json();
}

async function fetchText(url) {
  const r = await fetch(url, { headers: { "User-Agent": "pa-rail-board/1.0" } });
  if (!r.ok) throw new Error(`${url} -> HTTP ${r.status}`);
  return r.text();
}

async function septaAlerts() {
  // Official SEPTA Alerts API (verbose)
  // https://www3.septa.org/api/Alerts/get_alert_data.php  (systemwide)
  const url = "https://www3.septa.org/api/Alerts/get_alert_data.php";
  const data = await fetchJson(url);

  // Data shape can vary; keep it resilient:
  // Try to flatten arrays/objects into a list of alert objects.
  let alerts = [];
  if (Array.isArray(data)) alerts = data;
  else if (data?.alerts && Array.isArray(data.alerts)) alerts = data.alerts;
  else if (typeof data === "object") {
    // some endpoints return keyed-by-route structures
    alerts = Object.values(data).flat().filter(Boolean);
  }

  const rrLineNames = new Set(Object.keys(CENTROIDS).filter(k => k.endsWith("Line") || k.includes("/")));
  const items = [];

  for (const a of alerts) {
    const route = (a.route_name || a.route || a.route_id || "").toString();
    const message = (a.message || a.alert_message || a.header || a.description || "").toString();
    const severity = (a.severity || a.alert_type || "").toString();

    // Filter to Regional Rail-ish alerts:
    // We accept if route matches known RR lines OR message mentions "Regional Rail"
    const isRR =
      [...rrLineNames].some(n => route.toLowerCase().includes(n.toLowerCase())) ||
      /regional\s+rail/i.test(message);

    if (!isRR) continue;

    const loc = route || "SEPTA Regional Rail";
    const centroid = pickCentroid(loc);
    const etr = extractETR(message);

    items.push({
      railroad: "SEPTA",
      report_type: /delay|suspend|cancel|detour|shuttle|single track|signal|police|fire/i.test(message)
        ? "Disruption"
        : "Notice",
      location: loc,
      etr: etr || "",
      details: message ? message.slice(0, 220) : "",
      lat: centroid ? centroid[0] : null,
      lon: centroid ? centroid[1] : null,
      source_url: "https://www3.septa.org/api/Alerts/get_alert_data.php"
    });
  }

  return items;
}

async function amtrakAlerts() {
  // Official alerts page (HTML)
  const url = "https://www.amtrak.com/service-alerts-and-notices";
  const html = await fetchText(url);

  // Phase 1: simple keyword extraction for PA-relevant routes
  const keys = ["Keystone Service", "Pennsylvanian", "Northeast Regional", "Acela", "Philadelphia", "Harrisburg", "Pittsburgh"];
  const items = [];

  // very lightweight scrape: find lines around matching route names
  for (const k of ["Keystone Service", "Pennsylvanian", "Northeast Regional", "Acela"]) {
    const idx = html.toLowerCase().indexOf(k.toLowerCase());
    if (idx === -1) continue;

    // grab a small window around the match and strip tags crudely
    const snippet = html.slice(Math.max(0, idx - 250), Math.min(html.length, idx + 450));
    const text = snippet
      .replace(/<script[\s\S]*?<\/script>/gi, "")
      .replace(/<style[\s\S]*?<\/style>/gi, "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();

    const etr = extractETR(text);
    const centroid = CENTROIDS[k] || null;

    items.push({
      railroad: "Amtrak",
      report_type: "Notice",
      location: k,
      etr: etr || "",
      details: text.slice(0, 220),
      lat: centroid ? centroid[0] : null,
      lon: centroid ? centroid[1] : null,
      source_url: url
    });
  }

  return items;
}

async function main() {
  const [septa, amtrak] = await Promise.allSettled([septaAlerts(), amtrakAlerts()]);

  const items = []
    .concat(septa.status === "fulfilled" ? septa.value : [])
    .concat(amtrak.status === "fulfilled" ? amtrak.value : []);

  const out = {
    generated_at: new Date().toISOString(),
    count: items.length,
    items
  };

  await fs.mkdir("data", { recursive: true });
  await fs.writeFile(OUT, JSON.stringify(out, null, 2), "utf8");

  if (septa.status === "rejected") console.error("SEPTA fetch error:", septa.reason?.message || septa.reason);
  if (amtrak.status === "rejected") console.error("Amtrak fetch error:", amtrak.reason?.message || amtrak.reason);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
