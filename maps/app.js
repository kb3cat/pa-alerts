// PA Alerts — NWS-style Watches/Warnings/Advisories map
// Fixes "stuck loading" by adding:
// - fetch timeouts
// - limited concurrency for affectedZones geometry fallback
// - progress status updates

const statusText = document.getElementById("statusText");
const statusDot  = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");
const refreshBtn = document.getElementById("refreshBtn");

// Fixed window like the NWS product you showed (tweak if you want)
const VIEW_BOUNDS = L.latLngBounds(
  L.latLng(38.6, -81.0),  // SW
  L.latLng(42.9, -73.7)   // NE
);

// Map
const map = L.map("map", { zoomControl: true });
map.fitBounds(VIEW_BOUNDS);

// Muted basemap (you can remove this entirely if you want pure "blank + counties")
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
}).addTo(map);

// ---------- Status helpers ----------
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

// ---------- Hazard color mapping (event-name based like NWS products) ----------
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
  for (const h of HAZARD_COLORS) {
    if (eventLower.includes(h.match)) return h.color;
  }
  if (eventLower.includes("warning")) return TYPE_COLORS.warning;
  if (eventLower.includes("watch")) return TYPE_COLORS.watch;
  if (eventLower.includes("advisory")) return TYPE_COLORS.advisory;
  return "#60a5fa";
}

function featureStyle(feature){
  const p = feature?.properties || {};
  const eventLower = (p.event || "").toLowerCase();
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

// ---------- Legend ----------
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
    for (const e of activeEventsLowerSet) {
      if (e.includes(h.match)) return true;
    }
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

// ---------- County outlines + labels (darker) ----------
function computeBBoxFromCoords(coords) {
  // coords: Polygon => [ring[]], MultiPolygon => [[ring[]], ...]
  let minLat =  90, minLng =  180, maxLat = -90, maxLng = -180;

  const walkRing = (ring) => {
    for (const pt of ring) {
      const lng = pt[0], lat = pt[1];
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
    }
  };

  const walkPoly = (poly) => { for (const ring of poly) walkRing(ring); };

  // Polygon: coords = [ring, ring, ...]
  // MultiPolygon: coords = [[ring...], [ring...], ...]
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

function bboxIntersectsView(b) {
  if (!b) return false;
  const sw = VIEW_BOUNDS.getSouthWest();
  const ne = VIEW_BOUNDS.getNorthEast();
  return !(b.maxLng < sw.lng || b.minLng > ne.lng || b.maxLat < sw.lat || b.minLat > ne.lat);
}

async function loadCounties() {
  // NOTE: This is a large file. We cull features to VIEW_BOUNDS before adding.
  const url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json";
  try {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`County GeoJSON HTTP ${res.status}`);
    const geo = await res.json();

    const filtered = {
      type: "FeatureCollection",
      features: geo.features.filter(f => {
        const g = f.geometry;
        if (!g) return false;
        const bbox = computeBBoxFromCoords(g.coordinates);
        return bboxIntersectsView(bbox);
      })
    };

    const countiesLayer = L.geoJSON(filtered, {
      style: { color: "#0f172a", weight: 1.8, fillOpacity: 0 },

      onEachFeature: (feature, layer) => {
        const name = feature?.properties?.NAME;
        if (!name) return;

        // Show labels only when zoomed in enough to avoid clutter
        const shouldLabel = () => map.getZoom() >= 7;

        layer.bindTooltip(name, {
          permanent: shouldLabel(),
          direction: "center",
          className: "county-label"
        });

        layer.on("mouseover", () => layer.setStyle({ weight: 2.6, color: "#020617" }));
        layer.on("mouseout",  () => layer.setStyle({ weight: 1.8, color: "#0f172a" }));

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

// ---------- Alerts layer ----------
let alertLayer = L.geoJSON([], {
  style: featureStyle,
  onEachFeature: onEachAlert
}).addTo(map);

// ---------- Fetch helpers with timeout ----------
async function fetchJsonWithTimeout(url, ms, extraHeaders = {}) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), ms);

  try {
    const res = await fetch(url, {
      signal: controller.signal,
      headers: {
        Accept: "application/geo+json, application/json",
        "User-Agent": "PA Alerts Backup Map",
        ...extraHeaders
      }
    });
    if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

// ---------- Geometry fallback (fast + concurrency-limited) ----------
const zoneGeomCache = new Map();

// Limit how many zone URLs we’ll fetch per alert (prevents huge slowdowns)
const MAX_ZONES_PER_ALERT = 25;

// Limit global concurrency for zone fetches
const ZONE_FETCH_CONCURRENCY = 6;

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

async function fetchZoneGeometry(zoneUrl) {
  if (zoneGeomCache.has(zoneUrl)) return zoneGeomCache.get(zoneUrl);

  // 10s timeout per zone
  const zone = await fetchJsonWithTimeout(zoneUrl, 10000);
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
  if (!coords.length) return null;
  return { type: "MultiPolygon", coordinates: coords };
}

async function mapWithConcurrency(items, limit, mapper, onProgress) {
  const results = new Array(items.length);
  let i = 0;
  let done = 0;

  async function worker() {
    while (i < items.length) {
      const idx = i++;
      try {
        results[idx] = await mapper(items[idx], idx);
      } catch (e) {
        results[idx] = null;
      } finally {
        done++;
        if (onProgress) onProgress(done, items.length);
      }
    }
  }

  const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

async function ensureAlertGeometry(alertFeature, progressCb) {
  if (alertFeature?.geometry) return alertFeature;

  let zones = alertFeature?.properties?.affectedZones;
  if (!Array.isArray(zones) || zones.length === 0) return alertFeature;

  // Trim zones to keep it fast
  if (zones.length > MAX_ZONES_PER_ALERT) zones = zones.slice(0, MAX_ZONES_PER_ALERT);

  // Fetch zones with limited concurrency + progress updates
  const geoms = await mapWithConcurrency(
    zones,
    ZONE_FETCH_CONCURRENCY,
    async (z) => {
      const geom = await fetchZoneGeometry(z);
      if (!geom) return null;

      // Cull: only keep zone geoms that touch our view window
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

// ---------- Main fetch ----------
async function fetchAlerts() {
  setStatus("Loading alerts…", "loading");

  // 20s timeout for the main alerts call
  const data = await fetchJsonWithTimeout("https://api.weather.gov/alerts/active", 20000);
  const features = Array.isArray(data.features) ? data.features : [];

  // W/W/A only
  const wwa = features.filter(isWWA);

  const output = [];
  let processed = 0;

  for (const f of wwa) {
    processed++;
    setStatus(`Loading alerts… (${processed}/${wwa.length})`, "loading");

    // If it already has geometry, just use it
    if (f.geometry) {
      if (featureIntersectsBounds(f, VIEW_BOUNDS)) output.push(f);
      continue;
    }

    // Otherwise, build geometry from zones (with progress)
    const withGeom = await ensureAlertGeometry(f, (doneZones, totalZones) => {
      setStatus(`Loading alerts… (${processed}/${wwa.length}) zones ${doneZones}/${totalZones}`, "loading");
    });

    if (withGeom.geometry && featureIntersectsBounds(withGeom, VIEW_BOUNDS)) output.push(withGeom);
  }

  alertLayer.clearLayers();
  alertLayer.addData(output);

  // Keep the view fixed like the NWS product
  map.fitBounds(VIEW_BOUNDS.pad(0.02));

  // Update legend based on active events
  const activeEventsLower = new Set(output.map(f => ((f?.properties?.event) || "").toLowerCase()));
  updateLegend(activeEventsLower);

  setStatus(`Loaded ${output.length} alert(s).`, "ok");
  lastUpdated.textContent = `Updated: ${mmddyy_hhmmss(new Date())}`;
}

function handleErr(err) {
  console.error(err);
  setStatus(`Error loading alerts: ${err?.message || err}`, "error");
}

// UI
refreshBtn.addEventListener("click", () => fetchAlerts().catch(handleErr));

// Auto refresh every 5 minutes
setInterval(() => fetchAlerts().catch(handleErr), 300000);

// Initial load
fetchAlerts().catch(handleErr);
