#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";

const TOMTOM_KEY = process.env.TOMTOM_KEY;
const OUT_FILE = process.env.TQ_OUT_FILE || "data/tq_live.json";
const SAMPLE_EVERY_MILES = Number(process.env.TQ_SAMPLE_EVERY_MILES || 0.25);
const MAX_SAMPLE_POINTS = Number(process.env.TQ_MAX_SAMPLE_POINTS || 24);
const MAX_EVENTS = Number(process.env.TQ_MAX_EVENTS || 20);

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

function isCandidate(item, sourceFile){
  const text = `${textOf(item)} ${routeish(item)} ${item?.type || ""}`.toLowerCase();

  const majorRoad = /\b(i|us|pa)\s*[- ]?\s*\d{1,4}\b|turnpike|interstate/i.test(text);
  if (!majorRoad) return false;

  // In beta, include active closures and lane restrictions/major incidents.
  const useful =
    /clos|closed|closure|crash|disabled vehicle|lane restriction|incident|jackknifed|overturned|multi.vehicle|multi-vehicle/i.test(text);

  if (!useful) return false;

  // Avoid planned/future vehicle restriction noise for this beta.
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
      if (!isCandidate(item, file)) continue;

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

    result = 0; shift = 0;
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
    if (d < bestDist) { bestDist = d; best = i; }
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

  // upstream should generally be opposite travel direction, i.e. lower dot.
  return beforeDot < afterDot ? before : after;
}

function interpolate(a,b,f){
  return { lat: a.lat + (b.lat-a.lat)*f, lon: a.lon + (b.lon-a.lon)*f };
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
  try { return JSON.parse(text); }
  catch { throw new Error(`511PA incident ${id} did not return JSON.`); }
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
  if (ratio !== null && ratio <= 0.20) return "stopped";

  if (Number.isFinite(current) && current <= 35) return "slow";
  if (ratio !== null && ratio <= 0.60) return "slow";

  return "flowing";
}

function summarizeSamples(samples){
  let tqMiles = 0, backlogMiles = 0, sampledMiles = 0;
  let mode = "tq";

  for (let i=1; i<samples.length; i++) {
    const prev = samples[i-1];
    const cur = samples[i];
    const seg = milesBetween(prev.point, cur.point);
    sampledMiles += seg;

    if (mode === "tq") {
      if (cur.state === "stopped") { tqMiles += seg; continue; }
      if (cur.state === "slow") { mode = "backlog"; backlogMiles += seg; continue; }
      mode = "done"; continue;
    }

    if (mode === "backlog") {
      if (cur.state === "slow" || cur.state === "stopped") { backlogMiles += seg; continue; }
      mode = "done";
    }
  }

  return {
    tqMiles: Number(tqMiles.toFixed(2)),
    backlogMiles: Number(backlogMiles.toFixed(2)),
    totalAffectedMiles: Number((tqMiles + backlogMiles).toFixed(2)),
    sampledMiles: Number(sampledMiles.toFixed(2))
  };
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
    route, direction, description,
    type:item.type || "",
    isFullClosure:!!item.isFullClosure,
    lat, lon,
    secondaryLat:Number.isFinite(secondaryLat) ? secondaryLat : null,
    secondaryLon:Number.isFinite(secondaryLon) ? secondaryLon : null,
    polyline,
    linkIds:item.linkIds || ""
  };
}

async function analyzeIncident(discovered){
  const id = discovered.eventId || discovered;
  const raw = await fetch511Incident(id);
  const incident = normalizeIncident(raw,id);

  let geometry = decodePolyline(incident.polyline);

  if (geometry.length < 2 && incident.secondaryLat && incident.secondaryLon) {
    geometry = [
      {lat:incident.lat, lon:incident.lon},
      {lat:incident.secondaryLat, lon:incident.secondaryLon}
    ];
  }

  if (geometry.length < 2) geometry = [{lat:incident.lat, lon:incident.lon}];

  const incidentPoint = {lat:incident.lat, lon:incident.lon};
  const upstream = upstreamPath(geometry, incidentPoint, incident.direction);
  const points = samplePath(upstream, SAMPLE_EVERY_MILES, MAX_SAMPLE_POINTS);

  const samples = [];
  for (const point of points) {
    try {
      const flow = await fetchTomTomFlow(point);
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
      samples.push({ point, state:"unknown", error:String(e?.message || e) });
    }
  }

  const summary = summarizeSamples(samples);
  const usable = samples.filter(s => s.state !== "unknown");
  const avgConfidence = usable.length ? usable.reduce((sum,s)=>sum+Number(s.confidence||0),0)/usable.length : 0;

  let confidence = "low";
  if (usable.length >= 4 && avgConfidence >= 0.8) confidence = "high";
  else if (usable.length >= 3 && avgConfidence >= 0.5) confidence = "medium";

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
    secondaryPoint:incident.secondaryLat && incident.secondaryLon ? {lat:incident.secondaryLat, lon:incident.secondaryLon} : null,
    tqMiles:summary.tqMiles,
    backlogMiles:summary.backlogMiles,
    totalAffectedMiles:summary.totalAffectedMiles,
    sampledMiles:summary.sampledMiles,
    confidence,
    source:"511PA event geometry + TomTom live traffic flow",
    updated:nowIso(),
    samples
  };
}

async function ensureDir(file){ await fs.mkdir(path.dirname(file), {recursive:true}); }

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
      console.log(`OK ${result.eventId}: TQ ${result.tqMiles} mi, backlog ${result.backlogMiles} mi`);
    } catch(e) {
      results.push({ eventId:String(ev.eventId || ev), sourceFile:ev.source || null, error:String(e?.message || e), updated:nowIso() });
      console.error(`ERR ${ev.eventId || ev}:`, e?.message || e);
    }
  }

  const output = { name:"tq_live", fetched_at:nowIso(), count:results.length, events:results };
  await ensureDir(OUT_FILE);
  await fs.writeFile(OUT_FILE, JSON.stringify(output,null,2) + "\n", "utf8");
  console.log(`Wrote ${OUT_FILE}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
