#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";

const TOMTOM_KEY = process.env.TOMTOM_KEY;
const OUT_FILE = process.env.TQ_OUT_FILE || "data/tq_live.json";

const SAMPLE_EVERY_MILES = Number(process.env.TQ_SAMPLE_EVERY_MILES || 0.25);
const MAX_SAMPLE_POINTS = Number(process.env.TQ_MAX_SAMPLE_POINTS || 32);
const DOWNSTREAM_SAMPLE_POINTS = Number(process.env.TQ_DOWNSTREAM_SAMPLE_POINTS || 6);
const MAX_EVENTS = Number(process.env.TQ_MAX_EVENTS || 20);

const NORMAL_GAP_STOP_MILES = Number(process.env.TQ_NORMAL_GAP_STOP_MILES || 0.5);
const UNKNOWN_GAP_ALLOW_MILES = Number(process.env.TQ_UNKNOWN_GAP_ALLOW_MILES || 0.35);

const INPUT_FILES = (process.env.TQ_INPUT_FILES || "data/major_route_closures.json,data/lane_restrictions.json")
  .split(",").map(s => s.trim()).filter(Boolean);

const MANUAL_EVENT_IDS = (process.env.TQ_EVENT_IDS || process.env.EVENT_IDS || "")
  .split(",").map(s => s.trim()).filter(Boolean);

if (!TOMTOM_KEY) throw new Error("Missing TOMTOM_KEY environment variable.");

function nowIso(){ return new Date().toISOString(); }

async function readJsonIfExists(file){
  try { return JSON.parse(await fs.readFile(file, "utf8")); }
  catch { return null; }
}

function textOf(item){
  return String(item?.formatted || item?.description || item?.title || item?.text || "").trim();
}

function routeish(item){
  return String(item?.route || item?.roadway || textOf(item) || "").trim();
}

function extractId(item){
  const direct = item?.id || item?.eventId || item?.event_id || item?.alertId || item?.alert_id;
  if (direct) return String(direct).trim();

  const text = JSON.stringify(item || {});
  const m =
    text.match(/\bMajorRouteIncident-(\d{4,})\b/i) ||
    text.match(/\b(?:eventId|event_id|alertId|alert_id|id)["']?\s*[:=]\s*["']?(\d{4,})\b/i);
  return m ? m[1] : "";
}

function isCandidate(item){
  const text = `${textOf(item)} ${routeish(item)} ${item?.type || ""}`.toLowerCase();

  const majorRoad = /\b(i|us|pa)\s*[- ]?\s*\d{1,4}\b|turnpike|interstate/i.test(text);
  if (!majorRoad) return false;

  const useful =
    /clos|closed|closure|crash|disabled vehicle|lane restriction|incident|jackknifed|overturned|multi.vehicle|multi-vehicle/i.test(text);

  if (!useful) return false;

  if (/planned|future|starting on/i.test(text) && !/closure|closed|crash|incident/i.test(text)) return false;

  return true;
}

async function discoverEventIds(){
  const ids = new Map();

  for (const id of MANUAL_EVENT_IDS) {
    ids.set(String(id), { eventId:String(id), source:"manual" });
  }

  for (const file of INPUT_FILES) {
    const json = await readJsonIfExists(file);
    const items = Array.isArray(json?.items) ? json.items : [];

    for (const item of items) {
      const id = extractId(item);
      if (!id) continue;
      if (!isCandidate(item)) continue;

      if (!ids.has(id)) {
        ids.set(id, {
          eventId:id,
          source:file,
          sourceText:textOf(item),
          sourceRoute:routeish(item)
        });
      }
    }
  }

  return [...ids.values()].slice(0, MAX_EVENTS);
}

function milesBetween(a,b){
  const R = 3958.7613;
  const lat1 = a.lat * Math.PI/180, lat2 = b.lat * Math.PI/180;
  const dLat = (b.lat-a.lat) * Math.PI/180;
  const dLon = (b.lon-a.lon) * Math.PI/180;
  const x = Math.sin(dLat/2)**2 + Math.cos(lat1)*Math.cos(lat2)*Math.sin(dLon/2)**2;
  return 2 * R * Math.atan2(Math.sqrt(x), Math.sqrt(1-x));
}

function decodePolyline(str){
  if (!str || typeof str !== "string") return [];

  let index = 0, lat = 0, lon = 0;
  const points = [];

  while (index < str.length) {
    let result = 0, shift = 0, byte;

    do {
      byte = str.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < str.length);

    lat += (result & 1) ? ~(result >> 1) : (result >> 1);

    result = 0;
    shift = 0;

    do {
      byte = str.charCodeAt(index++) - 63;
      result |= (byte & 0x1f) << shift;
      shift += 5;
    } while (byte >= 0x20 && index < str.length);

    lon += (result & 1) ? ~(result >> 1) : (result >> 1);

    points.push({ lat: lat / 1e5, lon: lon / 1e5 });
  }

  return points;
}

function closestIndex(points, target){
  let best = 0, bestDist = Infinity;

  points.forEach((p,i) => {
    const d = milesBetween(p,target);
    if (d < bestDist) {
      bestDist = d;
      best = i;
    }
  });

  return best;
}

function directionVector(direction){
  const d = String(direction || "").toUpperCase();

  if (d.startsWith("N")) return {x:0,y:1};
  if (d.startsWith("S")) return {x:0,y:-1};
  if (d.startsWith("E")) return {x:1,y:0};
  if (d.startsWith("W")) return {x:-1,y:0};

  return {x:0,y:0};
}

function dotFromIncident(incident, endpoint, dir){
  const v = directionVector(dir);
  return ((endpoint.lon - incident.lon) * v.x) + ((endpoint.lat - incident.lat) * v.y);
}

function upstreamPath(points, incident, direction){
  if (points.length < 2) return points;

  const idx = closestIndex(points, incident);
  const before = points.slice(0, idx + 1).reverse();
  const after = points.slice(idx);

  const beforeEnd = before[before.length - 1] || before[0];
  const afterEnd = after[after.length - 1] || after[0];

  const beforeDot = dotFromIncident(incident, beforeEnd, direction);
  const afterDot = dotFromIncident(incident, afterEnd, direction);

  return beforeDot < afterDot ? before : after;
}

function downstreamPath(points, incident, direction){
  if (points.length < 2) return points;

  const idx = closestIndex(points, incident);
  const before = points.slice(0, idx + 1).reverse();
  const after = points.slice(idx);

  const beforeEnd = before[before.length - 1] || before[0];
  const afterEnd = after[after.length - 1] || after[0];

  const beforeDot = dotFromIncident(incident, beforeEnd, direction);
  const afterDot = dotFromIncident(incident, afterEnd, direction);

  return beforeDot >= afterDot ? before : after;
}

function interpolate(a,b,f){
  return {
    lat: a.lat + (b.lat-a.lat)*f,
    lon: a.lon + (b.lon-a.lon)*f
  };
}

function samplePath(points, everyMiles, maxPoints){
  if (!points || points.length < 2) return points || [];

  const samples = [points[0]];
  let distanceSinceLast = 0;

  for (let i=1; i<points.length; i++) {
    const a = points[i-1], b = points[i];
    const segMiles = milesBetween(a,b);
    if (segMiles <= 0) continue;

    let remaining = segMiles;

    while (distanceSinceLast + remaining >= everyMiles) {
      const need = everyMiles - distanceSinceLast;
      const fraction = (segMiles - remaining + need) / segMiles;

      samples.push(interpolate(a,b,fraction));

      if (samples.length >= maxPoints) return samples;

      remaining -= need;
      distanceSinceLast = 0;
    }

    distanceSinceLast += remaining;
  }

  return samples;
}

function normalizeTomTomCoordinate(c) {
  if (!c) return null;

  const lat = Number(c.latitude ?? c.lat);
  const lon = Number(c.longitude ?? c.lon ?? c.lng);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

  return { lat, lon };
}

function tomTomGeometryFromFlow(flow) {
  const coords =
    flow?.coordinates?.coordinate ||
    flow?.coordinates ||
    flow?.shape ||
    [];

  if (!Array.isArray(coords)) return [];

  return coords.map(normalizeTomTomCoordinate).filter(Boolean);
}

function pathLengthMiles(points) {
  if (!Array.isArray(points) || points.length < 2) return 0;

  let total = 0;
  for (let i = 1; i < points.length; i++) {
    total += milesBetween(points[i - 1], points[i]);
  }

  return total;
}

function uniquePoints(points) {
  const seen = new Set();
  const out = [];

  for (const p of points || []) {
    if (!p) continue;

    const key = `${p.lat.toFixed(6)},${p.lon.toFixed(6)}`;
    if (seen.has(key)) continue;

    seen.add(key);
    out.push(p);
  }

  return out;
}

function expandGeometryWithTomTomFlow(incidentPoint, direction, initialFlow) {
  const tomtomGeom = tomTomGeometryFromFlow(initialFlow);
  if (tomtomGeom.length < 2) return [];

  const upstream = upstreamPath(tomtomGeom, incidentPoint, direction);
  const sampled = samplePath(upstream, SAMPLE_EVERY_MILES, MAX_SAMPLE_POINTS);

  return uniquePoints(sampled);
}

async function fetch511Incident(id){
  const url = `https://www.511pa.com/map/data/MajorRouteIncident/${encodeURIComponent(id)}`;

  const r = await fetch(url, {
    headers: {
      "Accept": "application/json, text/javascript, */*; q=0.01",
      "User-Agent": "Mozilla/5.0 tq-live-beta",
      "X-Requested-With": "XMLHttpRequest",
      "Referer": "https://www.511pa.com/map"
    }
  });

  if (!r.ok) throw new Error(`511PA incident ${id} failed: ${r.status} ${r.statusText}`);

  const text = await r.text();

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`511PA incident ${id} did not return JSON.`);
  }
}

async function fetchTomTomFlow(point){
  const url =
    "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json" +
    `?key=${encodeURIComponent(TOMTOM_KEY)}` +
    `&point=${point.lat},${point.lon}` +
    "&unit=mph";

  const r = await fetch(url);

  if (!r.ok) throw new Error(`TomTom flow failed: ${r.status} ${r.statusText}`);

  const j = await r.json();
  return j.flowSegmentData || null;
}

function classifyFlow(flow){
  if (!flow) return "unknown";

  const current = Number(flow.currentSpeed);
  const free = Number(flow.freeFlowSpeed);
  const ratio = free > 0 ? current / free : null;

  if (flow.roadClosure === true) return "stopped";

  if (Number.isFinite(current) && current <= 10) return "stopped";
  if (ratio !== null && ratio <= 0.22) return "stopped";

  if (Number.isFinite(current) && current <= 25) return "severe";
  if (ratio !== null && ratio <= 0.40) return "severe";

  if (Number.isFinite(current) && current <= 40) return "slow";
  if (ratio !== null && ratio <= 0.65) return "slow";

  if (ratio !== null && ratio <= 0.80) return "moderate";

  return "flowing";
}

function isQueuedState(state){
  return state === "stopped" || state === "severe" || state === "slow";
}

function isHardQueueState(state){
  return state === "stopped" || state === "severe";
}

function averageFlowStats(samples) {
  const usable = (samples || []).filter(s =>
    s &&
    s.state !== "unknown" &&
    Number.isFinite(Number(s.currentSpeed)) &&
    Number.isFinite(Number(s.freeFlowSpeed)) &&
    Number(s.freeFlowSpeed) > 0
  );

  if (!usable.length) {
    return {
      usableCount: 0,
      avgSpeed: null,
      avgFreeFlow: null,
      avgRatio: null,
      stoppedCount: 0,
      severeCount: 0,
      slowCount: 0,
      moderateCount: 0,
      flowingCount: 0
    };
  }

  const sumSpeed = usable.reduce((sum, s) => sum + Number(s.currentSpeed), 0);
  const sumFree = usable.reduce((sum, s) => sum + Number(s.freeFlowSpeed), 0);

  const avgSpeed = sumSpeed / usable.length;
  const avgFreeFlow = sumFree / usable.length;
  const avgRatio = avgFreeFlow > 0 ? avgSpeed / avgFreeFlow : null;

  return {
    usableCount: usable.length,
    avgSpeed: Number(avgSpeed.toFixed(1)),
    avgFreeFlow: Number(avgFreeFlow.toFixed(1)),
    avgRatio: avgRatio == null ? null : Number(avgRatio.toFixed(2)),
    stoppedCount: usable.filter(s => s.state === "stopped").length,
    severeCount: usable.filter(s => s.state === "severe").length,
    slowCount: usable.filter(s => s.state === "slow").length,
    moderateCount: usable.filter(s => s.state === "moderate").length,
    flowingCount: usable.filter(s => s.state === "flowing").length
  };
}

function summarizeSamples(samples){
  let tqMiles = 0;
  let backlogMiles = 0;
  let totalAffectedMiles = 0;
  let sampledMiles = 0;

  let started = false;
  let normalGapMiles = 0;
  let unknownGapMiles = 0;

  for (let i=1; i<samples.length; i++) {
    const prev = samples[i-1];
    const cur = samples[i];

    const seg = milesBetween(prev.point, cur.point);
    sampledMiles += seg;

    if (isQueuedState(cur.state)) {
      started = true;
      normalGapMiles = 0;
      unknownGapMiles = 0;

      totalAffectedMiles += seg;

      if (isHardQueueState(cur.state)) {
        tqMiles += seg;
      } else {
        backlogMiles += seg;
      }

      continue;
    }

    if (cur.state === "unknown") {
      if (started && unknownGapMiles + seg <= UNKNOWN_GAP_ALLOW_MILES) {
        unknownGapMiles += seg;
        totalAffectedMiles += seg;
        backlogMiles += seg;
      }
      continue;
    }

    if (cur.state === "moderate") {
      if (started) {
        normalGapMiles = 0;
        unknownGapMiles = 0;
        totalAffectedMiles += seg;
        backlogMiles += seg;
      }
      continue;
    }

    if (cur.state === "flowing") {
      if (!started) continue;

      normalGapMiles += seg;

      if (normalGapMiles >= NORMAL_GAP_STOP_MILES) {
        break;
      }
    }
  }

  return {
    tqMiles: Number(tqMiles.toFixed(2)),
    backlogMiles: Number(backlogMiles.toFixed(2)),
    totalAffectedMiles: Number(totalAffectedMiles.toFixed(2)),
    sampledMiles: Number(sampledMiles.toFixed(2)),
    started
  };
}

function confidenceFromValidation(upstreamSamples, downstreamSamples, baseConfidence, summary) {
  const up = averageFlowStats(upstreamSamples);
  const down = averageFlowStats(downstreamSamples);

  let score = 0;

  if (baseConfidence === "high") score += 3;
  else if (baseConfidence === "medium") score += 2;
  else score += 1;

  if (up.usableCount >= 8) score += 2;
  else if (up.usableCount >= 4) score += 1;

  const upImpaired = up.stoppedCount + up.severeCount + up.slowCount;

  if (up.usableCount && upImpaired / up.usableCount >= 0.75) score += 2;
  else if (up.usableCount && upImpaired / up.usableCount >= 0.5) score += 1;

  if (summary.totalAffectedMiles >= 2) score += 1;
  if (summary.tqMiles >= 1) score += 1;

  if (down.usableCount >= 2 && up.avgRatio != null && down.avgRatio != null) {
    const ratioDelta = down.avgRatio - up.avgRatio;

    if (down.avgRatio >= 0.75 && ratioDelta >= 0.20) score += 2;
    else if (down.avgRatio >= 0.65 && ratioDelta >= 0.10) score += 1;

    if (down.avgRatio < 0.65 && Math.abs(ratioDelta) < 0.10) score -= 1;
  }

  if (score >= 7) return "high";
  if (score >= 4) return "medium";
  return "low";
}

function normalizeIncident(raw,id){
  const item = raw?.event || raw?.item || raw?.data || raw;

  const lat = Number(item.latitude ?? item.lat ?? item.Latitude);
  const lon = Number(item.longitude ?? item.lng ?? item.lon ?? item.Longitude);

  const secondaryLat = Number(item.secondaryLatitude ?? item.secondaryLat ?? item.toLatitude);
  const secondaryLon = Number(item.secondaryLongitude ?? item.secondaryLon ?? item.toLongitude);

  const polyline = item.polyline || item.encodedPolyline || item.routePolyline || "";
  const route = item.roadway || item.route || item.roadName || "";
  const direction = item.direction || item.dir || "";
  const description = item.description || item.title || item.formatted || "";

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    throw new Error(`Incident ${id} missing usable latitude/longitude.`);
  }

  return {
    id:String(item.id || id),
    route,
    direction,
    description,
    type:item.type || "",
    isFullClosure:!!item.isFullClosure,
    lat,
    lon,
    secondaryLat:Number.isFinite(secondaryLat) ? secondaryLat : null,
    secondaryLon:Number.isFinite(secondaryLon) ? secondaryLon : null,
    polyline,
    linkIds:item.linkIds || ""
  };
}

function buildGeometry(incident, incidentPoint, initialFlow) {
  let geometrySource = "511PA polyline";
  let geometry = decodePolyline(incident.polyline);

  if (geometry.length < 2 && incident.secondaryLat && incident.secondaryLon) {
    geometrySource = "511PA primary/secondary points";
    geometry = [
      {lat:incident.lat, lon:incident.lon},
      {lat:incident.secondaryLat, lon:incident.secondaryLon}
    ];
  }

  if (geometry.length < 2 && initialFlow) {
    const expanded = expandGeometryWithTomTomFlow(incidentPoint, incident.direction, initialFlow);

    if (expanded.length >= 2) {
      geometrySource = "TomTom flow segment geometry";
      geometry = expanded;
    }
  }

  if (geometry.length < 2) {
    geometrySource = "single incident point only";
    geometry = [incidentPoint];
  }

  return { geometry, geometrySource };
}

async function analyzeIncident(discovered){
  const id = discovered.eventId || discovered;
  const raw = await fetch511Incident(id);
  const incident = normalizeIncident(raw,id);

  const incidentPoint = {lat:incident.lat, lon:incident.lon};

  let initialFlow = null;

  try {
    initialFlow = await fetchTomTomFlow(incidentPoint);
  } catch {
    initialFlow = null;
  }

  const built = buildGeometry(incident, incidentPoint, initialFlow);
  const geometry = built.geometry;
  const geometrySource = built.geometrySource;

  const upstream = upstreamPath(geometry, incidentPoint, incident.direction);
  let points = samplePath(upstream, SAMPLE_EVERY_MILES, MAX_SAMPLE_POINTS);

  points = uniquePoints([incidentPoint, ...points]);

  const samples = [];

  for (const point of points) {
    try {
      const isIncidentPoint =
        Math.abs(point.lat - incidentPoint.lat) < 0.000001 &&
        Math.abs(point.lon - incidentPoint.lon) < 0.000001;

      const flow = (isIncidentPoint && initialFlow) ? initialFlow : await fetchTomTomFlow(point);

      samples.push({
        point,
        state: classifyFlow(flow),
        currentSpeed: flow?.currentSpeed ?? null,
        freeFlowSpeed: flow?.freeFlowSpeed ?? null,
        currentTravelTime: flow?.currentTravelTime ?? null,
        freeFlowTravelTime: flow?.freeFlowTravelTime ?? null,
        confidence: flow?.confidence ?? null,
        roadClosure: flow?.roadClosure ?? null
      });
    } catch (e) {
      samples.push({
        point,
        state:"unknown",
        error:String(e?.message || e)
      });
    }
  }

  const downstream = downstreamPath(geometry, incidentPoint, incident.direction);

  let downstreamPoints = samplePath(downstream, SAMPLE_EVERY_MILES, DOWNSTREAM_SAMPLE_POINTS);

  downstreamPoints = uniquePoints(downstreamPoints).filter(p =>
    Math.abs(p.lat - incidentPoint.lat) >= 0.000001 ||
    Math.abs(p.lon - incidentPoint.lon) >= 0.000001
  );

  const downstreamSamples = [];

  for (const point of downstreamPoints) {
    try {
      const flow = await fetchTomTomFlow(point);

      downstreamSamples.push({
        point,
        state: classifyFlow(flow),
        currentSpeed: flow?.currentSpeed ?? null,
        freeFlowSpeed: flow?.freeFlowSpeed ?? null,
        confidence: flow?.confidence ?? null,
        roadClosure: flow?.roadClosure ?? null
      });
    } catch (e) {
      downstreamSamples.push({
        point,
        state:"unknown",
        error:String(e?.message || e)
      });
    }
  }

  const summary = summarizeSamples(samples);

  const usable = samples.filter(s => s.state !== "unknown");
  const avgConfidence = usable.length
    ? usable.reduce((sum,s)=>sum+Number(s.confidence||0),0)/usable.length
    : 0;

  let baseConfidence = "low";

  if (usable.length >= 4 && avgConfidence >= 0.8) baseConfidence = "high";
  else if (usable.length >= 3 && avgConfidence >= 0.5) baseConfidence = "medium";

  const confidence = confidenceFromValidation(samples, downstreamSamples, baseConfidence, summary);

  let note = null;

  if (!usable.length) {
    note = "No usable upstream TomTom flow samples found; do not treat as a confirmed 0.00 mi queue.";
  } else if (!summary.started) {
    note = "Upstream flow samples were found, but no sustained queue was detected.";
  } else if (geometrySource === "single incident point only") {
    note = "Only the incident point was available; queue distance may be undercounted.";
  } else {
    note = "Directional upstream queue estimate using 511PA incident anchor and TomTom live flow.";
  }

  return {
    eventId:incident.id,
    route:incident.route,
    direction:incident.direction,
    type:incident.type,
    description:incident.description,
    isFullClosure:incident.isFullClosure,
    sourceFile:discovered.source || null,
    sourceText:discovered.sourceText || null,
    incidentPoint,
    secondaryPoint:incident.secondaryLat && incident.secondaryLon
      ? {lat:incident.secondaryLat, lon:incident.secondaryLon}
      : null,

    tqMiles:summary.tqMiles,
    backlogMiles:summary.backlogMiles,
    totalAffectedMiles:summary.totalAffectedMiles,
    sampledMiles:summary.sampledMiles,

    sampleCount:samples.length,
    geometrySource,
    geometryMiles:Number(pathLengthMiles(geometry).toFixed(2)),

    upstreamValidation: {
      stats: averageFlowStats(samples)
    },

    downstreamValidation: {
      sampleCount: downstreamSamples.length,
      stats: averageFlowStats(downstreamSamples)
    },

    confidence,
    note,
    source:`511PA event anchor + directional upstream TomTom live traffic flow; geometry source: ${geometrySource}`,
    updated:nowIso(),
    samples
  };
}

async function ensureDir(file){
  await fs.mkdir(path.dirname(file), {recursive:true});
}

async function main(){
  const discovered = await discoverEventIds();

  if (!discovered.length) {
    console.log("No active TQ candidate event IDs found.");
  } else {
    console.log(`Discovered ${discovered.length} TQ candidate event(s): ${discovered.map(x=>x.eventId).join(", ")}`);
  }

  const results = [];

  for (const ev of discovered) {
    try {
      const result = await analyzeIncident(ev);
      results.push(result);

      console.log(`OK ${result.eventId}: TQ ${result.tqMiles} mi, backlog ${result.backlogMiles} mi, confidence ${result.confidence}`);
    } catch(e) {
      results.push({
        eventId:String(ev.eventId || ev),
        sourceFile:ev.source || null,
        error:String(e?.message || e),
        updated:nowIso()
      });

      console.error(`ERR ${ev.eventId || ev}:`, e?.message || e);
    }
  }

  const output = {
    name:"tq_live",
    fetched_at:nowIso(),
    count:results.length,
    events:results
  };

  await ensureDir(OUT_FILE);
  await fs.writeFile(OUT_FILE, JSON.stringify(output,null,2) + "\n", "utf8");

  console.log(`Wrote ${OUT_FILE}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});