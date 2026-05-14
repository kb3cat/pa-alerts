#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";

const TOMTOM_KEY = process.env.TOMTOM_KEY;
const EVENT_IDS = (process.env.TQ_EVENT_IDS || process.env.EVENT_IDS || "")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);

const OUT_FILE = "data/tq_live.json";
const SAMPLE_EVERY_MILES = Number(process.env.TQ_SAMPLE_EVERY_MILES || 0.25);
const MAX_SAMPLE_POINTS = Number(process.env.TQ_MAX_SAMPLE_POINTS || 24);

if (!TOMTOM_KEY) {
  throw new Error("Missing TOMTOM_KEY environment variable.");
}

function nowIso() {
  return new Date().toISOString();
}

function milesBetween(a, b) {
  const R = 3958.7613;
  const lat1 = a.lat * Math.PI / 180;
  const lat2 = b.lat * Math.PI / 180;
  const dLat = (b.lat - a.lat) * Math.PI / 180;
  const dLon = (b.lon - a.lon) * Math.PI / 180;

  const x =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;

  return 2 * R * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
}

function decodePolyline(str) {
  if (!str || typeof str !== "string") return [];

  let index = 0;
  let lat = 0;
  let lon = 0;
  const points = [];

  while (index < str.length) {
    let result = 0;
    let shift = 0;
    let byte;

    do {
      byte = str.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < str.length);

    const dLat = result & 1 ? ~(result >> 1) : result >> 1;
    lat += dLat;

    result = 0;
    shift = 0;

    do {
      byte = str.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < str.length);

    const dLon = result & 1 ? ~(result >> 1) : result >> 1;
    lon += dLon;

    points.push({
      lat: lat / 1e5,
      lon: lon / 1e5
    });
  }

  return points;
}

function closestIndex(points, target) {
  let best = 0;
  let bestDist = Infinity;

  points.forEach((p, i) => {
    const d = milesBetween(p, target);
    if (d < bestDist) {
      bestDist = d;
      best = i;
    }
  });

  return best;
}

function directionVector(direction) {
  const d = String(direction || "").toUpperCase();

  if (d.startsWith("N")) return { x: 0, y: 1 };
  if (d.startsWith("S")) return { x: 0, y: -1 };
  if (d.startsWith("E")) return { x: 1, y: 0 };
  if (d.startsWith("W")) return { x: -1, y: 0 };

  return { x: 0, y: 0 };
}

function dotFromIncident(incident, endpoint, dir) {
  const v = directionVector(dir);
  return ((endpoint.lon - incident.lon) * v.x) + ((endpoint.lat - incident.lat) * v.y);
}

function upstreamPath(points, incident, direction) {
  if (points.length < 2) return points;

  const idx = closestIndex(points, incident);

  const before = points.slice(0, idx + 1).reverse();
  const after = points.slice(idx);

  const beforeEnd = before[before.length - 1] || before[0];
  const afterEnd = after[after.length - 1] || after[0];

  const beforeDot = dotFromIncident(incident, beforeEnd, direction);
  const afterDot = dotFromIncident(incident, afterEnd, direction);

  // Upstream should generally be opposite the travel direction, so dot < 0.
  if (beforeDot < afterDot) return before;
  return after;
}

function interpolate(a, b, fraction) {
  return {
    lat: a.lat + (b.lat - a.lat) * fraction,
    lon: a.lon + (b.lon - a.lon) * fraction
  };
}

function samplePath(points, everyMiles, maxPoints) {
  if (!points || points.length < 2) return points || [];

  const samples = [points[0]];
  let distanceSinceLast = 0;

  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1];
    const b = points[i];
    const segMiles = milesBetween(a, b);

    if (segMiles <= 0) continue;

    let remaining = segMiles;

    while (distanceSinceLast + remaining >= everyMiles) {
      const need = everyMiles - distanceSinceLast;
      const fraction = (segMiles - remaining + need) / segMiles;
      samples.push(interpolate(a, b, fraction));

      if (samples.length >= maxPoints) return samples;

      remaining -= need;
      distanceSinceLast = 0;
    }

    distanceSinceLast += remaining;
  }

  return samples;
}

async function fetch511Incident(id) {
  const url =  `https://www.511pa.com/map/data/MajorRouteIncident/${encodeURIComponent(id)}`;

  const r = await fetch(url, {
    headers: {
      "Accept": "application/json, text/javascript, */*; q=0.01",
      "User-Agent": "Mozilla/5.0 tq-live-beta",
      "X-Requested-With": "XMLHttpRequest",
      "Referer": "https://www.511pa.com/map"
    }
  });

  if (!r.ok) {
    throw new Error(`511PA incident ${id} failed: ${r.status} ${r.statusText}`);
  }

  const text = await r.text();

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`511PA incident ${id} did not return JSON.`);
  }
}

async function fetchTomTomFlow(point) {
  const url =
    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json" +
    `?key=${encodeURIComponent(TOMTOM_KEY)}` +
    `&point=${point.lat},${point.lon}` +
    "&unit=mph";

  const r = await fetch(url);

  if (!r.ok) {
    throw new Error(`TomTom flow failed: ${r.status} ${r.statusText}`);
  }

  const j = await r.json();
  return j.flowSegmentData || null;
}

function classifyFlow(flow) {
  if (!flow) return "unknown";

  const current = Number(flow.currentSpeed);
  const free = Number(flow.freeFlowSpeed);
  const ratio = free > 0 ? current / free : null;

  if (flow.roadClosure === true) return "stopped";
  if (Number.isFinite(current) && current <= 10) return "stopped";
  if (ratio !== null && ratio <= 0.2) return "stopped";

  if (Number.isFinite(current) && current <= 35) return "slow";
  if (ratio !== null && ratio <= 0.6) return "slow";

  return "flowing";
}

function summarizeSamples(samples) {
  let tqMiles = 0;
  let backlogMiles = 0;
  let totalMiles = 0;

  let mode = "tq";

  for (let i = 1; i < samples.length; i++) {
    const prev = samples[i - 1];
    const cur = samples[i];
    const seg = milesBetween(prev.point, cur.point);
    totalMiles += seg;

    if (mode === "tq") {
      if (cur.state === "stopped") {
        tqMiles += seg;
        continue;
      }

      if (cur.state === "slow") {
        mode = "backlog";
        backlogMiles += seg;
        continue;
      }

      mode = "done";
      continue;
    }

    if (mode === "backlog") {
      if (cur.state === "slow" || cur.state === "stopped") {
        backlogMiles += seg;
        continue;
      }

      mode = "done";
    }
  }

  return {
    tqMiles: Number(tqMiles.toFixed(2)),
    backlogMiles: Number(backlogMiles.toFixed(2)),
    totalAffectedMiles: Number((tqMiles + backlogMiles).toFixed(2)),
    sampledMiles: Number(totalMiles.toFixed(2))
  };
}

function normalizeIncident(raw, id) {
  const item = raw?.event || raw?.item || raw?.data || raw;

  const lat =
    Number(item.latitude) ||
    Number(item.lat) ||
    Number(item.Latitude);

  const lon =
    Number(item.longitude) ||
    Number(item.lng) ||
    Number(item.lon) ||
    Number(item.Longitude);

  const secondaryLat =
    Number(item.secondaryLatitude) ||
    Number(item.secondaryLat) ||
    Number(item.toLatitude);

  const secondaryLon =
    Number(item.secondaryLongitude) ||
    Number(item.secondaryLon) ||
    Number(item.toLongitude);

  const polyline = item.polyline || item.encodedPolyline || item.routePolyline || "";

  const route = item.roadway || item.route || item.roadName || "";
  const direction = item.direction || item.dir || "";
  const description = item.description || item.title || item.formatted || "";

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error(`Incident ${id} missing usable latitude/longitude.`);
  }

  return {
    id: String(item.id || id),
    route,
    direction,
    description,
    isFullClosure: !!item.isFullClosure,
    lat,
    lon,
    secondaryLat: Number.isFinite(secondaryLat) ? secondaryLat : null,
    secondaryLon: Number.isFinite(secondaryLon) ? secondaryLon : null,
    polyline,
    linkIds: item.linkIds || ""
  };
}

async function analyzeIncident(id) {
  const raw = await fetch511Incident(id);
  const incident = normalizeIncident(raw, id);

  let geometry = decodePolyline(incident.polyline);

  if (geometry.length < 2 && incident.secondaryLat && incident.secondaryLon) {
    geometry = [
      { lat: incident.lat, lon: incident.lon },
      { lat: incident.secondaryLat, lon: incident.secondaryLon }
    ];
  }

  if (geometry.length < 2) {
    geometry = [{ lat: incident.lat, lon: incident.lon }];
  }

  const incidentPoint = { lat: incident.lat, lon: incident.lon };
  const upstream = upstreamPath(geometry, incidentPoint, incident.direction);
  const points = samplePath(upstream, SAMPLE_EVERY_MILES, MAX_SAMPLE_POINTS);

  const samples = [];

  for (const point of points) {
    try {
      const flow = await fetchTomTomFlow(point);
      const state = classifyFlow(flow);

      samples.push({
        point,
        state,
        currentSpeed: flow?.currentSpeed ?? null,
        freeFlowSpeed: flow?.freeFlowSpeed ?? null,
        confidence: flow?.confidence ?? null,
        roadClosure: flow?.roadClosure ?? null
      });
    } catch (e) {
      samples.push({
        point,
        state: "unknown",
        error: String(e?.message || e)
      });
    }
  }

  const summary = summarizeSamples(samples);

  let confidence = "low";
  const usable = samples.filter(s => s.state !== "unknown");
  const avgConfidence = usable.length
    ? usable.reduce((sum, s) => sum + Number(s.confidence || 0), 0) / usable.length
    : 0;

  if (usable.length >= 4 && avgConfidence >= 0.8) confidence = "high";
  else if (usable.length >= 3 && avgConfidence >= 0.5) confidence = "medium";

  return {
    eventId: incident.id,
    route: incident.route,
    direction: incident.direction,
    description: incident.description,
    isFullClosure: incident.isFullClosure,

    incidentPoint,
    secondaryPoint: incident.secondaryLat && incident.secondaryLon
      ? { lat: incident.secondaryLat, lon: incident.secondaryLon }
      : null,

    tqMiles: summary.tqMiles,
    backlogMiles: summary.backlogMiles,
    totalAffectedMiles: summary.totalAffectedMiles,
    sampledMiles: summary.sampledMiles,

    confidence,
    source: "511PA event geometry + TomTom live traffic flow",
    updated: nowIso(),

    samples
  };
}

async function ensureDir(file) {
  await fs.mkdir(path.dirname(file), { recursive: true });
}

async function main() {
  if (!EVENT_IDS.length) {
    throw new Error("No event IDs provided. Set TQ_EVENT_IDS, example: TQ_EVENT_IDS=254168");
  }

  const results = [];

  for (const id of EVENT_IDS) {
    try {
      const result = await analyzeIncident(id);
      results.push(result);
      console.log(`OK ${id}: TQ ${result.tqMiles} mi, backlog ${result.backlogMiles} mi`);
    } catch (e) {
      results.push({
        eventId: String(id),
        error: String(e?.message || e),
        updated: nowIso()
      });
      console.error(`ERR ${id}:`, e?.message || e);
    }
  }

  const output = {
    name: "tq_live",
    fetched_at: nowIso(),
    count: results.length,
    events: results
  };

  await ensureDir(OUT_FILE);
  await fs.writeFile(OUT_FILE, JSON.stringify(output, null, 2) + "\n", "utf8");

  console.log(`Wrote ${OUT_FILE}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
