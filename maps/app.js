// PA Alerts — NWS-style W/W/A map (PA + surrounding window)
// Adds: county labels, major highways/US routes (reference overlay), PA state highlight
// Fixes: "frozen loading" with time budgets, global zone cap, concurrency, aborting prior run

const statusText  = document.getElementById("statusText");
const statusDot   = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");
const refreshBtn  = document.getElementById("refreshBtn");

// Fixed window similar to your screenshot (tweak anytime)
const VIEW_BOUNDS = L.latLngBounds(
  L.latLng(38.6, -81.0),  // SW
  L.latLng(42.9, -73.7)   // NE
);

// --- Map ---
const map = L.map("map", { zoomControl: true });
map.fitBounds(VIEW_BOUNDS);

// Basemap: ESRI Light Gray + Reference overlay
// Reference layer includes major roads/highways & labels in a clean way.
L.tileLayer(
  "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
  { maxZoom: 18, attribution: "Esri" }
).addTo(map);

L.tileLayer(
  "https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Reference/MapServer/tile/{z}/{y}/{x}",
  { maxZoom: 18, attribution: "Esri" }
).addTo(map);

// --- Status helpers ---
function setStatus(text, mode = "idle") {
  statusText.textContent = text;
  const colors = { idle:"#6b7280", loading:"#f59e0b", ok:"#22c55e", error:"#ef4444" };
  statusDot.style.background = colors[mode] || colors.idle;
}

function mmddyy_hhmmss(date) {
  const mm = String(date.getMonth()+1).padStart(2,"0");
  const dd = String(date.getDate()).padStart(2,"0");
  const yy = String(date.getFullYear()).slice(-2);
  const hh = String(date.getHours()).padStart(2,"0");
  const mi = String(date.getMinutes()).padStart(2,"0");
  const ss = String(date.getSeconds()).padStart(2,"0");
  return `${mm}/${dd}/${yy} ${hh}:${mi}:${ss}`;
}

function escapeHtml(str){
  return String(str).replace(/[&<>"']/g, s => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
  }[s]));
}
function escapeAttr(str){ return String(str).replace(/"/g, "%22"); }

// --- Hazard coloring (event-name based like the NWS product feel) ---
const HAZARD_COLORS = [
  { match: "marine dense fog advisory", color: "#475569", label: "Marine Dense Fog Advisory" },
  { match: "dense fog advisory",        color: "#64748b", label: "Dense Fog Advisory" },
  { match: "winter weather advisory",   color: "#7c3aed", label: "Winter Weather Advisory" }
];

const TYPE_COLORS = {
  warning:  "#ef4444",
  watch:    "#f59e0b",
  advisory: "#64748b"
};

function isWWA(feature) {
  const event = ((feature?.properties?.event) || "").toLowerCase();
  return event.includes("warning") || event.includes("watch") || event.includes("advisory");
}

function getHazardColor(eventLower){
  for (const h of HAZARD_COLORS) if (eventLower.includes(h.match)) return h.color;
  if (eventLower.includes("warning")) return TYPE_COLORS.warning;
  if (eventLower.includes("watch")) return TYPE_COLORS.watch;
  if (eventLower.includes("advisory")) return TYPE_COLORS.advisory;
  return "#60a5fa";
}

function featureStyle(feature){
  const eventLower = ((feature?.properties?.event) || "").toLowerCase();
  const color = getHazardColor(eventLower);
  return { color, weight: 2, fillColor: color, fillOpacity: 0.35 };
}

function onEachAlert(feature, layer){
  const p = feature.properties || {};
  const effective = p.effective ? new Date(p.effective) : null;
  const expires   = p.expires ? new Date(p.expires) : null;

  const html = `
    <div class="popup-title">${escapeHtml(p.event || "NWS Alert")}</div>
    <div class="popup-row"><span class="popup-muted">Area:</span> ${escapeHtml(p.areaDesc || "")}</div>
    <div class="popup-row">
      <span class="popup-muted">Severity:</span> ${escapeHtml(p.severity || "Unknown")}
      &nbsp; <span class="popup-muted">Urgency:</span> ${escapeHtml(p.urgency || "Unknown")}
      &nbsp; <span class="popup-muted">Certainty:</span> ${escapeHtml(p.certainty || "Unknown")}
    </div>
    <div class="popup-row"><span class="popup-muted">Effective:</span> ${effective ? escapeHtml(effective.toLocaleString()) : "—"}</div>
    <div class="popup-row"><span class="popup-muted">Expires:</span> ${expires ? escapeHtml(expires.toLocaleString()) : "—"}</div>
    ${p.web ? `<div class="popup-row"><a href="${escapeAttr(p.web)}" target="_blank" rel="noopener">View alert</a></div>` : ""}
  `;
  layer.bindPopup(html, { maxWidth: 460 });
}

// --- Legend ---
const legend = L.control({ position: "bottomleft" });
legend.onAdd = function () {
  const div = L.DomUtil.create("div", "legend");
  div.innerHTML = `<div class="title">Legend</div><div id="legendRows"></div>`;
  return div;
};
legend.addTo(map);

function updateLegend(activeEventsLowerSet) {
  const rows = document.getElementById("legendRows");
  if (!rows) return;

  const active = HAZARD_COLORS.filter(h => {
    for (const e of activeEventsLowerSet) if (e.includes(h.match)) return true;
    return false;
  });

  if (active.length) {
    rows.innerHTML = active.map(h => `
      <div class="row">
        <span class="swatch" style="background:${h.color}"></span>
        <span>${h.label}</span>
      </div>
    `).join("");
  } else {
    rows.innerHTML = `
      <div class="row"><span class="swatch" style="background:${TYPE_COLORS.warning}"></span><span>Warning</span></div>
      <div class="row"><span class="swatch" style="background:${TYPE_COLORS.watch}"></span><span>Watch</span></div>
      <div class="row"><span class="swatch" style="background:${TYPE_COLORS.advisory}"></span><span>Advisory</span></div>
    `;
  }
}

// --- Helpers: quick intersects without building Leaflet layers for everything ---
function bboxFromCoords(coords) {
  let minLat =  90, minLng =  180, maxLat = -90, maxLng = -180;

  const walkRing = (ring) => {
    for (const [lng, lat] of ring) {
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
    }
  };

  const walkPoly = (poly) => { for (const ring of poly) walkRing(ring); };

  if (!Array.isArray(coords) || coords.length === 0) return null;

  // Polygon: [ring[]...]
  if (Array.isArray(coords[0]) && Array.isArray(coords[0][0]) && typeof coords[0][0][0] === "number") {
    walkPoly(coords);
  } else {
    // MultiPolygon: [[ring[]...], ...]
    for (const poly of coords) walkPoly(poly);
  }

  return { minLat, minLng, maxLat, maxLng };
}

function bboxIntersectsView(b) {
  if (!b) return false;
  const sw = VIEW_BOUNDS.getSouthWest();
  const ne = VIEW_BOUNDS.getNorthEast();
  return !(b.maxLng < sw.lng || b.minLng > ne.lng || b.maxLat < sw.lat || b.minLat > ne.lat);
}

function featureIntersectsBounds(feature, bounds) {
  try {
    const tmp = L.geoJSON(feature);
    const b = tmp.getBounds();
    tmp.remove();
    return b.isValid() && b.intersects(bounds);
  } catch {
    return false;
  }
}

// --- County outlines + labels (darker; labels appear at zoom >= 7) ---
async function loadCounties() {
  try {
    const url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json";
    const res = await fetch(url);
    if (!res.ok) throw new Error(`County GeoJSON HTTP ${res.status}`);
    const geo = await res.json();

    // Cull counties to our view window before adding (faster)
    const filtered = {
      type: "FeatureCollection",
      features: geo.features.filter(f => {
        const g = f.geometry;
        if (!g) return false;
        const bb = bboxFromCoords(g.coordinates);
        return bboxIntersectsView(bb);
      })
    };

    const countiesLayer = L.geoJSON(filtered, {
      style: { color: "#0b1220", weight: 2.0, fillOpacity: 0 },
      onEachFeature: (feature, layer) => {
        const name = feature?.properties?.NAME;
        if (!name) return;

        const shouldLabel = () => map.getZoom() >= 7;

        layer.bindTooltip(name, {
          permanent: shouldLabel(),
          direction: "center",
          className: "county-label"
        });

        layer.on("mouseover", () => layer.setStyle({ weight: 2.8, color: "#020617" }));
        layer.on("mouseout",  () => layer.setStyle({ weight: 2.0, color: "#0b1220" }));

        map.on("zoomend", () => {
          const tt = layer.getTooltip();
          if (!tt) return;
          if (shouldLabel()) layer.openTooltip();
          else layer.closeTooltip();
        });
      }
    });

    countiesLayer.addTo(map);
  } catch (e) {
    console.warn("County layer failed:", e);
  }
}
loadCounties();

// --- PA state highlight (thick outline + subtle fill) ---
async function loadStateHighlight() {
  try {
    const url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/us-states.json";
    const res = await fetch(url);
    if (!res.ok) throw new Error(`States GeoJSON HTTP ${res.status}`);
    const geo = await res.json();

    const layer = L.geoJSON(geo, {
      filter: (f) => (f?.properties?.name === "Pennsylvania"),
      style: {
        color: "#020617",
        weight: 4,
        fillColor: "#0b1220",
        fillOpacity: 0.08
      }
    });

    layer.addTo(map);
  } catch (e) {
    console.warn("State highlight failed:", e);
  }
}
loadStateHighlight();

// --- Alerts layer ---
let alertLayer = L.geoJSON([], { style: featureStyle, onEachFeature: onEachAlert }).addTo(map);

// --- Fetch controls / anti-freeze ---
let currentRun = { abort: null, inProgress: false };

// Limits (these prevent “stuck loading”)
const MAIN_ALERTS_TIMEOUT_MS = 20000;
const ZONE_TIMEOUT_MS = 8000;
const OVERALL_TIME_BUDGET_MS = 25000;     // stop and render what we have
const GLOBAL_ZONE_FETCH_CAP = 60;         // total zone URL fetches per refresh
const ZONE_FETCH_CONCURRENCY = 6;         // parallel zone fetches
const MAX_ZONES_PER_ALERT = 20;           // cap zones per alert

const zoneGeomCache = new Map();          // url -> geometry|null

function makeAbortable() {
  if (currentRun.abort) currentRun.abort.abort();
  const abort = new AbortController();
  currentRun.abort = abort;
  return abort;
}

async function fetchJsonWithTimeout(url, ms, signal) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);

  const combined = signal
    ? new AbortController()
    : controller;

  // If we have an external signal, "combine" by aborting combined when either aborts
  if (signal) {
    const onAbort = () => { try { combined.abort(); } catch {} };
    signal.addEventListener("abort", onAbort, { once: true });
    controller.signal.addEventListener("abort", onAbort, { once: true });
  }

  try {
    const res = await fetch(url, {
      signal: combined.signal,
      headers: {
        Accept: "application/geo+json, application/json",
        "User-Agent": "PA Alerts Backup Map"
      }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

async function mapWithConcurrency(items, limit, mapper, progressCb) {
  const results = new Array(items.length);
  let i = 0, done = 0;

  async function worker() {
    while (i < items.length) {
      const idx = i++;
      try { results[idx] = await mapper(items[idx], idx); }
      catch { results[idx] = null; }
      finally {
        done++;
        if (progressCb) progressCb(done, items.length);
      }
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
  return results;
}

async function fetchZoneGeometry(zoneUrl, signal) {
  if (zoneGeomCache.has(zoneUrl)) return zoneGeomCache.get(zoneUrl);

  const zone = await fetchJsonWithTimeout(zoneUrl, ZONE_TIMEOUT_MS, signal);
  const geom = zone?.geometry || null;

  zoneGeomCache.set(zoneUrl, geom);
  return geom;
}

function mergeToMultiPolygon(geoms) {
  const coords = [];
  for (const g of geoms) {
    if (!g || !g.type || !g.coordinates) continue;
    if (g.type === "Polygon") coords.push(g.coordinates);
    else if (g.type === "MultiPolygon") for (const poly of g.coordinates) coords.push(poly);
  }
  return coords.length ? { type: "MultiPolygon", coordinates: coords } : null;
}

async function ensureAlertGeometry(alertFeature, signal, counters, progressCb) {
  if (alertFeature?.geometry) return alertFeature;

  let zones = alertFeature?.properties?.affectedZones;
  if (!Array.isArray(zones) || zones.length === 0) return alertFeature;

  zones = zones.slice(0, MAX_ZONES_PER_ALERT);

  // Respect global cap
  const remaining = Math.max(0, GLOBAL_ZONE_FETCH_CAP - counters.zoneFetches);
  if (remaining <= 0) return alertFeature;
  zones = zones.slice(0, remaining);

  const geoms = await mapWithConcurrency(
    zones,
    ZONE_FETCH_CONCURRENCY,
    async (z) => {
      if (counters.zoneFetches >= GLOBAL_ZONE_FETCH_CAP) return null;
      counters.zoneFetches++;

      const geom = await fetchZoneGeometry(z, signal);
      if (!geom) return null;

      // Cull to view window
      const test = { type: "Feature", geometry: geom, properties: {} };
      if (!featureIntersectsBounds(test, VIEW_BOUNDS)) return null;

      return geom;
    },
    progressCb
  );

  const merged = mergeToMultiPolygon(geoms.filter(Boolean));
  if (!merged) return alertFeature;

  return { ...alertFeature, geometry: merged };
}

async function fetchAlerts() {
  const abort = makeAbortable();
  currentRun.inProgress = true;

  const start = Date.now();
  const counters = { zoneFetches: 0 };

  try {
    setStatus("Loading alerts…", "loading");

    const data = await fetchJsonWithTimeout("https://api.weather.gov/alerts/active", MAIN_ALERTS_TIMEOUT_MS, abort.signal);
    const features = Array.isArray(data.features) ? data.features : [];

    const wwa = features.filter(isWWA);

    const output = [];
    let processed = 0;

    for (const f of wwa) {
      processed++;

      // Hard time budget to prevent “forever loading”
      if (Date.now() - start > OVERALL_TIME_BUDGET_MS) break;

      setStatus(`Loading alerts… (${processed}/${wwa.length}) zones:${counters.zoneFetches}/${GLOBAL_ZONE_FETCH_CAP}`, "loading");

      // If alert has geometry, cheap path
      if (f.geometry) {
        if (featureIntersectsBounds(f, VIEW_BOUNDS)) output.push(f);
        continue;
      }

      // Otherwise fallback to zones (bounded)
      const withGeom = await ensureAlertGeometry(
        f,
        abort.signal,
        counters,
        (doneZones, totalZones) => {
          setStatus(`Loading alerts… (${processed}/${wwa.length}) zones ${doneZones}/${totalZones} (${counters.zoneFetches}/${GLOBAL_ZONE_FETCH_CAP})`, "loading");
        }
      );

      if (withGeom.geometry && featureIntersectsBounds(withGeom, VIEW_BOUNDS)) output.push(withGeom);
    }

    alertLayer.clearLayers();
    alertLayer.addData(output);

    // Keep view fixed like the NWS product
    map.fitBounds(VIEW_BOUNDS.pad(0.02));

    const activeEventsLower = new Set(output.map(f => ((f?.properties?.event) || "").toLowerCase()));
    updateLegend(activeEventsLower);

    const partialNote = (Date.now() - start > OVERALL_TIME_BUDGET_MS) ? " (partial)" : "";
    setStatus(`Loaded ${output.length} alert(s).${partialNote}`, "ok");
    lastUpdated.textContent = `Updated: ${mmddyy_hhmmss(new Date())}`;
  } catch (err) {
    if (String(err).includes("AbortError")) {
      setStatus("Load canceled (refresh started).", "idle");
    } else {
      console.error(err);
      setStatus(`Error loading alerts: ${err?.message || err}`, "error");
    }
  } finally {
    currentRun.inProgress = false;
  }
}

refreshBtn.addEventListener("click", () => fetchAlerts());

// Auto refresh every 5 minutes
setInterval(() => fetchAlerts(), 300000);

// Initial load
fetchAlerts();
