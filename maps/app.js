// PA Alerts — "NWS-style" Watches/Warnings/Advisories map
// Goal: fixed PA regional extent + hazard-colored shading + legend

const statusText = document.getElementById("statusText");
const statusDot = document.getElementById("statusDot");
const lastUpdated = document.getElementById("lastUpdated");
const refreshBtn = document.getElementById("refreshBtn");

// Fixed extent similar to the screenshot window (tweak if you want tighter/looser)
const VIEW_BOUNDS = L.latLngBounds(
  L.latLng(38.6, -81.0),  // SW
  L.latLng(42.9, -73.7)   // NE
);

// --- Basemap (muted / cleaner than default OSM)
// Carto "Positron" is a common light basemap and reads like NWS products.
// If you prefer to stick with OSM, tell me and I’ll swap it back.
const map = L.map("map", { zoomControl: true });
map.fitBounds(VIEW_BOUNDS);

L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
  maxZoom: 18,
  attribution: '&copy; OpenStreetMap contributors &copy; CARTO'
}).addTo(map);

// --- Hazard color mapping (this is the key to “copying” the NWS map feel)
// These are intentionally simple and readable.
// Adjust hex codes if you want to match the screenshot more tightly.
const HAZARD_COLORS = [
  { match: "winter weather advisory", color: "#7c3aed", label: "Winter Weather Advisory" },
  { match: "dense fog advisory",      color: "#64748b", label: "Dense Fog Advisory" },
  { match: "marine dense fog advisory", color: "#475569", label: "Marine Dense Fog Advisory" }
];

// Fallback colors by type
const TYPE_COLORS = {
  warning: "#ef4444",
  watch: "#f59e0b",
  advisory: "#64748b"
};

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

// Determine hazard color (NWS-style: color by hazard name, not severity)
function getHazardColor(eventLower) {
  for (const h of HAZARD_COLORS) {
    if (eventLower.includes(h.match)) return h.color;
  }

  // Fallback based on type words
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

// Intersect check so we ONLY keep alerts that touch your map window
function intersectsView(feature) {
  try {
    const tmp = L.geoJSON(feature);
    const b = tmp.getBounds();
    tmp.remove();
    return b.isValid() && b.intersects(VIEW_BOUNDS);
  } catch {
    return false;
  }
}

// --- Layers
let alertLayer = L.geoJSON([], {
  style: featureStyle,
  pointToLayer: (feature, latlng) => L.circleMarker(latlng, {
    radius: 7,
    weight: 2,
    fillOpacity: 0.75
  }),
  onEachFeature: onEachAlert
}).addTo(map);

// --- Legend (like the NWS map)
const legend = L.control({ position: "bottomleft" });

legend.onAdd = function() {
  const div = L.DomUtil.create("div", "legend");
  div.innerHTML = `<div class="title">Legend</div><div id="legendRows"></div>`;
  return div;
};

legend.addTo(map);

function updateLegend(activeEventsLowerSet) {
  const rows = document.getElementById("legendRows");
  if (!rows) return;

  // Only show legend entries that are actually active in the current view
  const active = HAZARD_COLORS.filter(h => {
    for (const e of activeEventsLowerSet) {
      if (e.includes(h.match)) return true;
    }
    return false;
  });

  // If none match the explicit hazards, still show generic W/W/A swatches
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

  // 1) W/W/A only
  // 2) ONLY those that touch the fixed view window
  const filtered = features
    .filter(isWWA)
    .filter(intersectsView);

  alertLayer.clearLayers();
  alertLayer.addData(filtered);

  // Keep your view consistent like the NWS product (don’t auto-zoom to data)
  map.fitBounds(VIEW_BOUNDS.pad(0.02));

  // Update legend based on what’s currently active
  const activeEventsLower = new Set(
    filtered.map(f => ((f?.properties?.event) || "").toLowerCase())
  );
  updateLegend(activeEventsLower);

  setStatus(`Loaded ${filtered.length} alert(s).`, "ok");
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
