// PA Alerts — "NWS-style" Watches/Warnings/Advisories map (regional window)
// FIX: If alerts have no geometry, fetch polygons from affectedZones and draw them.

const statusText = document.getElementById("statusText");
const statusDot = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");
const refreshBtn = document.getElementById("refreshBtn");

// Fixed extent similar to the screenshot window (tweak later if needed)
const VIEW_BOUNDS = L.latLngBounds(
  L.latLng(38.6, -81.0),  // SW
  L.latLng(42.9, -73.7)   // NE
);

// Map
const map = L.map("map", { zoomControl: true });
map.fitBounds(VIEW_BOUNDS);

// Muted basemap (reads more like NWS products)
L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
}).addTo(map);

// Hazard color mapping (event-name based like the NWS product feel)
// You can tweak these hex values after you see it live.
const HAZARD_COLORS = [
  { match: "winter weather advisory",        color: "#7c3aed", label: "Winter Weather Advisory" },
  { match: "dense fog advisory",             color: "#64748b", label: "Dense Fog Advisory" },
  { match: "marine dense fog advisory",      color: "#475569", label: "Marine Dense Fog Advisory" }
];

const TYPE_COLORS = {
  warning: "#ef4444",
  watch: "#f59e0b",
  advisory: "#64748b"
};

// Zone geometry cache (keyed by zone URL)
const zoneGeomCache = new Map();

function setStatus(text, mode = "idle") {
  statusText.textContent = text;
  const colors = { idle:"#6b7280", loading:"#f59e0b", ok:"#22c55e", error:"#ef4444" };
  statusDot.style.background = colors[mode] || colors.idle;
}

function mmddyy_hhmmss(date) {
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const yy = String(date.getFullYear()).slice(-2);
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  return `${mm}/${dd}/${yy} ${hh}:${mi}:${ss}`;
}

function isWWA(feature) {
  const event = ((feature?.properties?.event) || "").toLowerCase();
  return event.includes("warning") || event.includes("watch") || event.includes("advisory");
}

function getHazardColor(eventLower) {
  for (const h of HAZARD_COLORS) {
    if (eventLower.includes(h.match)) return h.color;
  }
  if (eventLower.includes("warning")) return TYPE_COLORS.warning;
  if (eventLower.includes("watch")) return TYPE_COLORS.watch;
  if (eventLower.includes("advisory")) return TYPE_COLORS.advisory;
  return "#60a5fa";
}

function featureStyle(feature) {
  const p = feature?.properties || {};
  const eventLower = (p.event || "").toLowerCase();
  const color = getHazardColor(eventLower);

  return {
    color,
    weight: 2,
    fillColor: color,
    fillOpacity: 0.35
  };
}

function onEachAlert(feature, layer) {
  const p = feature.properties || {};
  const title = p.event || "NWS Alert";
  const areaDesc = p.areaDesc || "";
  const severity = p.severity || "Unknown";
  const urgency = p.urgency || "Unknown";
  const certainty = p.certainty || "Unknown";

  const effective = p.effective ? new Date(p.effective) : null;
  const expires = p.expires ? new Date(p.expires) : null;

  const html = `
    <div class="popup-title">${escapeHtml(title)}</div>
    <div class="popup-row"><span class="popup-muted">Area:</span> ${escapeHtml(areaDesc)}</div>
    <div class="popup-row">
      <span class="popup-muted">Severity:</span> ${escapeHtml(severity)}
      &nbsp; <span class="popup-muted">Urgency:</span> ${escapeHtml(urgency)}
      &nbsp; <span class="popup-muted">Certainty:</span> ${escapeHtml(certainty)}
    </div>
    <div class="popup-row"><span class="popup-muted">Effective:</span> ${effective ? escapeHtml(effective.toLocaleString()) : "—"}</div>
    <div class="popup-row"><span class="popup-muted">Expires:</span> ${expires ? escapeHtml(expires.toLocaleString()) : "—"}</div>
    ${p.web ? `<div class="popup-row"><a href="${escapeAttr(p.web)}" target="_blank" rel="noopener">View alert</a></div>` : ""}
  `;
  layer.bindPopup(html, { maxWidth: 460 });
}

function escapeHtml(str) {
  return String(str).replace(/[&<>"']/g, s => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
  }[s]));
}
function escapeAttr(str) {
  return String(str).replace(/"/g, "%22");
}

// Legend
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

  const htmlParts = [];
  if (active.length) {
    for (const h of active) {
      htmlParts.push(`
        <div class="row">
          <span class="swatch" style="background:${h.color}"></span>
          <span>${h.label}</span>
        </div>
      `);
    }
  } else {
    htmlParts.push(`
      <div class="row"><span class="swatch" style="background:${TYPE_COLORS.warning}"></span><span>Warning</span></div>
      <div class="row"><span class="swatch" style="background:${TYPE_COLORS.watch}"></span><span>Watch</span></div>
      <div class="row"><span class="swatch" style="background:${TYPE_COLORS.advisory}"></span><span>Advisory</span></div>
    `);
  }

  rows.innerHTML = htmlParts.join("");
}

// Bounds intersection helper that works with GeoJSON features
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

// ---- Zone geometry fallback ----
//
// If alert.geometry is null, use alert.properties.affectedZones (array of URLs)
// Each zone endpoint typically has geometry; we collect them into a MultiPolygon.
//
// Notes:
// - We cache zone geometries to reduce calls.
// - We only fetch zones that intersect the VIEW_BOUNDS to keep it fast.

async function fetchZoneGeometry(zoneUrl) {
  if (zoneGeomCache.has(zoneUrl)) return zoneGeomCache.get(zoneUrl);

  const res = await fetch(zoneUrl, {
    headers: {
      Accept: "application/geo+json, application/json",
      "User-Agent": "PA Alerts Backup Map"
    }
  });
  if (!res.ok) throw new Error(`Zone fetch failed ${res.status} for ${zoneUrl}`);

  const zone = await res.json();
  const geom = zone?.geometry || null;

  zoneGeomCache.set(zoneUrl, geom);
  return geom;
}

function mergePolygonsToMultiPolygon(geoms) {
  // Accept Polygon or MultiPolygon geometries, return one MultiPolygon or null
  const coords = [];

  for (const g of geoms) {
    if (!g || !g.type || !g.coordinates) continue;

    if (g.type === "Polygon") {
      coords.push(g.coordinates);
    } else if (g.type === "MultiPolygon") {
      for (const poly of g.coordinates) coords.push(poly);
    }
  }

  if (!coords.length) return null;
  return { type: "MultiPolygon", coordinates: coords };
}

async function ensureAlertHasGeometry(alertFeature) {
  // If it already has geometry, keep it
  if (alertFeature?.geometry) return alertFeature;

  const zones = alertFeature?.properties?.affectedZones;
  if (!Array.isArray(zones) || zones.length === 0) return alertFeature;

  // Fetch zone geometries; limit concurrency a bit by doing it sequentially (simple + safe)
  const zoneGeoms = [];

  for (const z of zones) {
    try {
      const geom = await fetchZoneGeometry(z);
      if (!geom) continue;

      // Quick cull: only keep zone geometry if it intersects our view window
      const testFeature = { type: "Feature", geometry: geom, properties: {} };
      if (featureIntersectsBounds(testFeature, VIEW_BOUNDS)) {
        zoneGeoms.push(geom);
      }
    } catch (e) {
      console.warn("Zone geometry error:", e);
    }
  }

  const merged = mergePolygonsToMultiPolygon(zoneGeoms);
  if (!merged) return alertFeature;

  return {
    ...alertFeature,
    geometry: merged
  };
}

// ---- Layers ----
let alertLayer = L.geoJSON([], {
  style: featureStyle,
  onEachFeature: onEachAlert
}).addTo(map);

async function fetchAlerts() {
  setStatus("Loading alerts…", "loading");

  const url = "https://api.weather.gov/alerts/active";
  const res = await fetch(url, {
    headers: {
      Accept: "application/geo+json, application/json",
      "User-Agent": "PA Alerts Backup Map"
    }
  });
  if (!res.ok) throw new Error(`NWS API HTTP ${res.status}`);

  const data = await res.json();
  const features = Array.isArray(data.features) ? data.features : [];

  // W/W/A only first
  const wwa = features.filter(isWWA);

  // Ensure geometry (zone fallback) then keep only those intersecting our window
  const withGeom = [];
  for (const f of wwa) {
    const g = await ensureAlertHasGeometry(f);
    if (g?.geometry && featureIntersectsBounds(g, VIEW_BOUNDS)) {
      withGeom.push(g);
    }
  }

  alertLayer.clearLayers();
  alertLayer.addData(withGeom);

  // Keep the view fixed like the NWS product
  map.fitBounds(VIEW_BOUNDS.pad(0.02));

  const activeEventsLower = new Set(withGeom.map(f => ((f?.properties?.event) || "").toLowerCase()));
  updateLegend(activeEventsLower);

  setStatus(`Loaded ${withGeom.length} alert(s).`, "ok");
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
