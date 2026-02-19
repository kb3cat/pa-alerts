// PA Alerts — NWS Watches/Warnings/Advisories map
// Strategy:
//  - Fetch all active alerts (api.weather.gov/alerts/active)
//  - Filter to: Warning/Watch/Advisory ONLY
//  - Then filter to a "Region" bounding box (PA + surrounding states)
//  - Fit map to that region (similar idea to the NWS regional graphic)

const statusText = document.getElementById("statusText");
const statusDot = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");

const viewSelect = document.getElementById("viewSelect");
const refreshBtn = document.getElementById("refreshBtn");

// Regional extent (covers PA + surrounding states)
// southwest (lat, lng) and northeast (lat, lng)
const REGION_BOUNDS = L.latLngBounds(
  L.latLng(37.0, -83.8),  // SW: roughly KY/WV edge
  L.latLng(43.8, -73.0)   // NE: up into NY / VT-ish edge
);

// PA-ish bounds (tighter)
const PA_BOUNDS = L.latLngBounds(
  L.latLng(39.5, -80.7),
  L.latLng(42.6, -74.2)
);

// Initialize map with a regional view (like the NWS product style)
const map = L.map("map", { zoomControl: true });
map.fitBounds(REGION_BOUNDS);

// Basemap: OSM is fine; if you want it closer to that “muted” NWS look,
// tell me and I’ll swap tiles to a gray canvas style.
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

// Alerts layer
let alertLayer = L.geoJSON([], {
  style: featureStyle,
  pointToLayer: (feature, latlng) => L.circleMarker(latlng, {
    radius: 7,
    weight: 2,
    fillOpacity: 0.75
  }),
  onEachFeature: onEachAlert
}).addTo(map);

function setStatus(text, mode = "idle") {
  statusText.textContent = text;
  const colors = {
    idle: "#6b7280",
    loading: "#f59e0b",
    ok: "#22c55e",
    error: "#ef4444"
  };
  statusDot.style.background = colors[mode] || colors.idle;
}

// mm/dd/yy timestamp (your preference)
function mmddyy_hhmmss(date) {
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  const yy = String(date.getFullYear()).slice(-2);
  const hh = String(date.getHours()).padStart(2, "0");
  const mi = String(date.getMinutes()).padStart(2, "0");
  const ss = String(date.getSeconds()).padStart(2, "0");
  return `${mm}/${dd}/${yy} ${hh}:${mi}:${ss}`;
}

// Locked filter: Warning / Watch / Advisory only
function isWWA(feature) {
  const event = ((feature?.properties?.event) || "").toLowerCase();
  return event.includes("warning") || event.includes("watch") || event.includes("advisory");
}

// Quick "does this alert touch these bounds?" check
// Uses Leaflet's GeoJSON bounds computation (works for Polygons + MultiPolygons)
function intersectsBounds(feature, bounds) {
  try {
    const tmp = L.geoJSON(feature);
    const b = tmp.getBounds();
    tmp.remove();
    return b.isValid() && b.intersects(bounds);
  } catch {
    return false;
  }
}

// Styling: closer to “ops readable” than “pretty”.
// If you want to mimic NWS colors per hazard (e.g., Dense Fog Advisory gray, Winter purple),
// say the word and I’ll add a hazard color dictionary.
function featureStyle(feature) {
  const p = feature?.properties || {};
  const event = (p.event || "").toLowerCase();

  // Type-based base colors
  let color = "#f59e0b"; // watch default (orange)
  if (event.includes("warning")) color = "#ef4444";       // red
  else if (event.includes("advisory")) color = "#64748b"; // slate/gray like your screenshot vibe
  else if (event.includes("watch")) color = "#f59e0b";    // orange/yellow

  // A couple common tweaks for winter-style look
  if (event.includes("winter") || event.includes("snow") || event.includes("ice")) {
    color = "#7c3aed"; // purple-ish
  }

  return {
    color,
    weight: 2,
    fillColor: color,
    fillOpacity: 0.28
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
  layer.bindPopup(html, { maxWidth: 440 });
}

function escapeHtml(str) {
  return String(str).replace(/[&<>"']/g, s => ({
    "&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#039;"
  }[s]));
}
function escapeAttr(str) {
  return String(str).replace(/"/g, "%22");
}

function getViewMode() {
  return (viewSelect?.value || "region").toLowerCase();
}

function getViewBounds(mode) {
  if (mode === "pa") return PA_BOUNDS;
  if (mode === "all") return null;
  return REGION_BOUNDS;
}

async function fetchAlerts() {
  const mode = getViewMode();
  const bounds = getViewBounds(mode);

  setStatus(`Loading alerts (${mode})…`, "loading");

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

  // Filter to W/W/A only
  let filtered = features.filter(isWWA);

  // Filter to bounds if region/pa
  if (bounds) {
    filtered = filtered.filter(f => intersectsBounds(f, bounds));
  }

  alertLayer.clearLayers();
  alertLayer.addData(filtered);

  // Fit map to bounds (prefer the chosen view bounds, otherwise data bounds)
  if (bounds) {
    map.fitBounds(bounds.pad(0.04));
  } else {
    const b = alertLayer.getBounds();
    if (b.isValid()) map.fitBounds(b.pad(0.06));
  }

  setStatus(`Loaded ${filtered.length} alert(s).`, "ok");
  lastUpdated.textContent = `Updated: ${mmddyy_hhmmss(new Date())}`;
}

function handleErr(err) {
  console.error(err);
  setStatus(`Error loading alerts: ${err?.message || err}`, "error");
}

// UI events
refreshBtn.addEventListener("click", () => fetchAlerts().catch(handleErr));
viewSelect.addEventListener("change", () => fetchAlerts().catch(handleErr));

// Auto refresh every 5 minutes
setInterval(() => fetchAlerts().catch(handleErr), 300000);

// Initial load
fetchAlerts().catch(handleErr);
