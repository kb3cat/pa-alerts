// PA Alerts — PA zones only, unique color per alert type (event)
// Legend shows ONLY active alert types currently present on the map
// Also: darker labeled county outlines, major highways, PA state highlight
// Anti-freeze: time budget + zone caps + concurrency + abort prior run

const statusText  = document.getElementById("statusText");
const statusDot   = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");
const refreshBtn  = document.getElementById("refreshBtn");

// PA-focused view window
const VIEW_BOUNDS = L.latLngBounds(
  L.latLng(39.5, -80.7), // SW
  L.latLng(42.6, -74.2)  // NE
);

// --- Map ---
const map = L.map("map", { zoomControl: true });
map.fitBounds(VIEW_BOUNDS);
map.setZoom(map.getZoom() + 1); // zoom in a step

// Basemap: ESRI Light Gray + Reference overlay (major roads + labels)
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

// --- W/W/A only ---
function isWWA(feature) {
  const event = ((feature?.properties?.event) || "").toLowerCase();
  return event.includes("warning") || event.includes("watch") || event.includes("advisory");
}

// --- PA forecast zones only (for fallback geometry) ---
function isPAForecastZoneUrl(url) {
  // Typical: https://api.weather.gov/zones/forecast/PAZ071
  return typeof url === "string" && /\/zones\/forecast\/PAZ\d{3}$/i.test(url);
}

// --- Unique color per event type (stable hashing) ---
function hashStringToHue(str) {
  // simple deterministic hash
  let h = 0;
  for (let i = 0; i < str.length; i++) h = (h * 31 + str.charCodeAt(i)) >>> 0;
  return h % 360;
}
function eventToColor(eventName) {
  // HSL gives us lots of distinct colors without maintaining a huge map
  const hue = hashStringToHue(eventName.toLowerCase());
  // Keep them readable on light basemap: medium saturation, medium-dark
  return `hsl(${hue} 75% 45%)`;
}

// --- Styling + popup ---
function featureStyle(feature){
  const event = (feature?.properties?.event) || "NWS Alert";
  const color = eventToColor(event);
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

// --- Legend: only active event types ---
const legend = L.control({ position: "bottomleft" });
legend.onAdd = function () {
  const div = L.DomUtil.create("div", "legend");
  div.innerHTML = `<div class="title">Active Alerts</div><div id="legendRows"></div>`;
  return div;
};
legend.addTo(map);

function updateLegendFromEvents(eventCounts) {
  const rows = document.getElementById("legendRows");
  if (!rows) return;

  const entries = Array.from(eventCounts.entries())
    .sort((a,b) => b[1]-a[1] || a[0].localeCompare(b[0]));

  if (!entries.length) {
    rows.innerHTML = `<div class="row"><span class="swatch" style="background:#64748b"></span><span>None</span></div>`;
    return;
  }

  rows.innerHTML = entries.map(([eventName, count]) => {
    const color = eventToColor(eventName);
    return `
      <div class="row">
        <span class="swatch" style="background:${color}"></span>
        <span>${escapeHtml(eventName)} (${count})</span>
      </div>
    `;
  }).join("");
}

// --- County outlines + labels (PA only via bounds cull) ---
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

  if (Array.isArray(coords[0]) && Array.isArray(coords[0][0]) && typeof coords[0][0][0] === "number") {
    // Polygon
    walkPoly(coords);
  } else {
    // MultiPolygon
    for (const poly of coords) walkPoly(poly);
  }

  return { minLat, minLng, maxLat, maxLng };
}
function bboxIntersectsBounds(b, bounds) {
  if (!b) return false;
  const sw = bounds.getSouthWest();
  const ne = bounds.getNorthEast();
  return !(b.maxLng < sw.lng || b.minLng > ne.lng || b.maxLat < sw.lat || b.minLat > ne.lat);
}

async function loadCounties() {
  try {
    const url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json";
    const res = await fetch(url);
    if (!res.ok) throw new Error(`County GeoJSON HTTP ${res.status}`);
    const geo = await res.json();

    const filtered = {
      type: "FeatureCollection",
      features: geo.features.filter(f => {
        const g = f.geometry;
        if (!g) return false;
        const bb = bboxFromCoords(g.coordinates);
        return bboxIntersectsBounds(bb, VIEW_BOUNDS);
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

// --- PA highlight (dark outline + subtle fill) ---
async function loadStateHighlight() {
  try {
    const url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/us-states.json";
    const res = await fetch(url);
    if (!res.ok) throw new Error(`States GeoJSON HTTP ${res.status}`);
    const geo = await res.json();

    L.geoJSON(geo, {
      filter: (f) => (f?.properties?.name === "Pennsylvania"),
      style: { color: "#020617", weight: 4, fillColor: "#0b1220", fillOpacity: 0.08 }
    }).addTo(map);
  } catch (e) {
    console.warn("State highlight failed:", e);
  }
}
loadStateHighlight();

// --- Alerts layer ---
let alertLayer = L.geoJSON([], { style: featureStyle, onEachFeature: onEachAlert }).addTo(map);

// --- Anti-freeze controls ---
let currentAbort = null;

const MAIN_ALERTS_TIMEOUT_MS = 15000;
const ZONE_TIMEOUT_MS        = 7000;
const OVERALL_TIME_BUDGET_MS = 20000; // render what we have by then
const GLOBAL_ZONE_FETCH_CAP  = 50;    // total zone fetches per refresh
const ZONE_FETCH_CONCURRENCY = 6;
const MAX_ZONES_PER_ALERT    = 15;

const zoneGeomCache = new Map(); // zoneUrl -> geometry|null

function abortPreviousRun() {
  if (currentAbort) currentAbort.abort();
  currentAbort = new AbortController();
  return currentAbort;
}

async function fetchJsonWithTimeout(url, ms, signal) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);

  const combined = new AbortController();
  const abortBoth = () => { try { combined.abort(); } catch {} };
  if (signal) signal.addEventListener("abort", abortBoth, { once: true });
  controller.signal.addEventListener("abort", abortBoth, { once: true });

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

  // PA forecast zones only
  zones = zones.filter(isPAForecastZoneUrl);
  if (zones.length === 0) return alertFeature;

  // caps
  zones = zones.slice(0, MAX_ZONES_PER_ALERT);
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

// --- Main fetch (PA only) ---
async function fetchAlerts() {
  const abort = abortPreviousRun();
  const start = Date.now();
  const counters = { zoneFetches: 0 };

  try {
    setStatus("Loading PA alerts…", "loading");

    const data = await fetchJsonWithTimeout(
      "https://api.weather.gov/alerts/active?area=PA",
      MAIN_ALERTS_TIMEOUT_MS,
      abort.signal
    );

    const features = Array.isArray(data.features) ? data.features : [];
    const wwa = features.filter(isWWA);

    const output = [];
    let processed = 0;

    for (const f of wwa) {
      processed++;

      // time budget safety
      if (Date.now() - start > OVERALL_TIME_BUDGET_MS) break;

      setStatus(`Loading PA alerts… (${processed}/${wwa.length}) zones:${counters.zoneFetches}/${GLOBAL_ZONE_FETCH_CAP}`, "loading");

      if (f.geometry) {
        if (featureIntersectsBounds(f, VIEW_BOUNDS)) output.push(f);
        continue;
      }

      const withGeom = await ensureAlertGeometry(
        f,
        abort.signal,
        counters,
        (doneZones, totalZones) => {
          setStatus(`Loading PA alerts… (${processed}/${wwa.length}) zones ${doneZones}/${totalZones} (${counters.zoneFetches}/${GLOBAL_ZONE_FETCH_CAP})`, "loading");
        }
      );

      if (withGeom.geometry && featureIntersectsBounds(withGeom, VIEW_BOUNDS)) output.push(withGeom);
    }

    // Render
    alertLayer.clearLayers();
    alertLayer.addData(output);

    // Update legend/key: ONLY active event types
    const counts = new Map();
    for (const f of output) {
      const e = (f?.properties?.event) || "NWS Alert";
      counts.set(e, (counts.get(e) || 0) + 1);
    }
    updateLegendFromEvents(counts);

    const partial = (Date.now() - start > OVERALL_TIME_BUDGET_MS) ? " (partial)" : "";
    setStatus(`Loaded ${output.length} alert(s).${partial}`, "ok");
    lastUpdated.textContent = `Updated: ${mmddyy_hhmmss(new Date())}`;
  } catch (err) {
    if (String(err).includes("AbortError")) {
      setStatus("Load canceled (refresh started).", "idle");
    } else {
      console.error(err);
      setStatus(`Error loading alerts: ${err?.message || err}`, "error");
    }
  }
}

refreshBtn.addEventListener("click", () => fetchAlerts());
setInterval(() => fetchAlerts(), 300000);
fetchAlerts();
