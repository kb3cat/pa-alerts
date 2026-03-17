import { chromium } from "playwright";
import fs from "fs";

async function scrapeSimpleTable(url, tableSelector = "table") {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  await page.goto(url, { waitUntil: "networkidle" });

  for (const sel of [
    'button:has-text("Done")',
    'button:has-text("Next")',
    'button[aria-label="Close"]'
  ]) {
    try { await page.click(sel, { timeout: 1500 }); } catch {}
  }

  await page.waitForTimeout(1500);

  const headers = await page.$$eval(`${tableSelector} thead th`, ths =>
    ths.map(th => th.innerText.trim())
  );

  const rows = await page.$$eval(`${tableSelector} tbody tr`, trs =>
    trs.map(tr => Array.from(tr.querySelectorAll("td")).map(td => td.innerText.trim()))
  );

  await browser.close();

  return { url, fetched_at: new Date().toISOString(), headers, rows };
}

/* ---------- HELPERS ---------- */

function idx(headers, name) {
  const i = headers.findIndex(h => h.trim().toLowerCase() === name.trim().toLowerCase());
  return i >= 0 ? i : null;
}

function findHeader(headers, re) {
  const i = headers.findIndex(h => re.test(String(h || "")));
  return i >= 0 ? i : null;
}

function norm(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function parseRoute(desc) {
  const m = String(desc || "").match(/\b(I|US|PA)\s*[- ]?\s*(\d{1,3})\b/i);
  if (!m) return null;
  return `${m[1].toUpperCase()}-${m[2]}`;
}

function parseDirection(desc) {
  const m1 = String(desc || "").match(/\b(NB|SB|EB|WB)\b/i);
  if (m1) return m1[1].toUpperCase();

  const m2 = String(desc || "").match(/\b(NORTH|SOUTH|EAST|WEST)(BOUND)?\b/i);
  if (!m2) return null;
  return m2[1].toUpperCase();
}

function parseBetweenExits(desc) {
  const d = String(desc || "");

  const mA = d.match(/between\s+Exit\s+(\d+[A-Z]?)\s*:\s*([^]+?)\s+(?:and|to)\s+Exit\s+(\d+[A-Z]?)\s*:\s*([^]+?)(?:\.|$)/i);
  if (mA) {
    return {
      from: `Exit ${mA[1]}: ${norm(mA[2])}`,
      to: `Exit ${mA[3]}: ${norm(mA[4])}`
    };
  }

  const mB = d.match(/between\s+Exit\s*:?\s*([^()]+?)\s*\((\d+[A-Z]?)\)\s*(?:and|to)\s*([^()]+?)\s*\((\d+[A-Z]?)\)/i);
  if (mB) {
    return {
      from: `${norm(mB[1])} (${mB[2]})`,
      to: `${norm(mB[3])} (${mB[4]})`
    };
  }

  return null;
}

function isConstructionRelated(desc) {
  return /(roadwork|construction|work zone|lane closure for work|paving|bridge|maintenance|utility work|shoulder work)/i.test(desc);
}

function isAllLanesClosedOrBlocked(desc) {
  return /\ball lanes (closed|blocked)\b/i.test(desc);
}

function isAllLanesOpen(desc) {
  return /\ball lanes open\b/i.test(desc);
}

function parseCountyFromDesc(desc) {
  const parts = String(desc || "")
    .split("|")
    .map(s => s.trim())
    .filter(Boolean);

  for (let i = 0; i < parts.length - 1; i++) {
    if (/^pennsylvania$/i.test(parts[i])) {
      return parts[i + 1].replace(/\s*county$/i, "").trim();
    }
  }

  const m = String(desc || "").match(/\b([A-Za-z .'-]+)\s+County\b/i);
  if (m) return m[1].trim();

  return null;
}

function extractNarrativeFromDesc(desc) {
  const raw = String(desc || "");
  if (!raw.includes("|")) return norm(raw);

  const parts = raw
    .split("|")
    .map(s => s.trim())
    .filter(Boolean);

  const nonTime = parts.filter(
    p => !/^\d{1,2}\/\d{1,2}\/\d{2,4}\s*,\s*\d{1,2}:\d{2}\s*(AM|PM)$/i.test(p)
  );

  const paIdx = nonTime.findIndex(p => /^pennsylvania$/i.test(p));
  const countyToken = (paIdx >= 0 && nonTime[paIdx + 1]) ? nonTime[paIdx + 1] : null;

  const cleaned = nonTime.filter(p => {
    const t = p.trim();

    if (/^closure\b/i.test(t)) return false;
    if (/^restriction\b/i.test(t)) return false;
    if (/^event\b/i.test(t)) return false;
    if (/^major route$/i.test(t)) return false;
    if (/^pennsylvania$/i.test(t)) return false;
    if (countyToken && t.toLowerCase() === countyToken.toLowerCase()) return false;
    if (parseRoute(t)) return false;

    return true;
  });

  return norm(cleaned[0] || raw);
}

function parseReopenToMMDDYY_HHMM(endRaw) {
  const s = norm(endRaw);
  if (!s) return "TBD";

  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})\s*,\s*(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (!m) return "TBD";

  const mo = String(parseInt(m[1], 10));
  const da = String(parseInt(m[2], 10));
  const yy = String(m[3]).slice(-2);

  let hh = parseInt(m[4], 10);
  const mm = String(m[5]).padStart(2, "0");
  const ap = m[6].toUpperCase();

  if (ap === "AM") {
    if (hh === 12) hh = 0;
  } else {
    if (hh !== 12) hh += 12;
  }

  const hh2 = String(hh).padStart(2, "0");
  return `${mo}/${da}/${yy} - ${hh2}:${mm}`;
}

function normalizeNarrativeText(s) {
  let t = String(s || "")
    .replace(/\bMulti\s+vehicle\b/gi, "Multi-vehicle")
    .replace(/\ball lanes (closed|blocked)\b/gi, (m, w) => `All lanes ${w[0].toUpperCase()}${w.slice(1).toLowerCase()}`);

  if (t && !/[.!?]$/.test(t)) t += ".";
  return t;
}

/* ---------- MAJOR ROUTE CLOSURES ---------- */

function buildMajorRouteClosures(trafficTable) {
  const headers = trafficTable.headers || [];
  const rows = trafficTable.rows || [];

  const typeIdx = idx(headers, "Type") ?? findHeader(headers, /type/i);
  const descIdx = idx(headers, "Description") ?? findHeader(headers, /description/i);
  const endIdx  = idx(headers, "Anticipated End Time") ?? findHeader(headers, /(anticipated|end)/i);
  const countyIdx =
    idx(headers, "County") ??
    idx(headers, "County Name") ??
    findHeader(headers, /\bcounty\b/i);

  const items = [];

  for (const r of rows) {
    const type = norm(typeIdx != null ? r[typeIdx] : "");
    const desc = norm(descIdx != null ? r[descIdx] : "");
    const end  = norm(endIdx != null ? r[endIdx] : "");

    if (!desc) continue;

    const isMajorRoute = /major route/i.test(type) || /major route/i.test(desc);
    if (!isMajorRoute) continue;

    const isClosure = /\bclosure\b/i.test(type) || /\bclosure\b/i.test(desc);
    if (!isClosure) continue;

    if (isConstructionRelated(desc)) continue;
    if (isAllLanesOpen(desc)) continue;
    if (!isAllLanesClosedOrBlocked(desc)) continue;

    const route = parseRoute(desc) || "ROUTE";
    const dir = parseDirection(desc) || "DIRECTION";
    const between = parseBetweenExits(desc);

    const countyFromCol = countyIdx != null ? norm(r[countyIdx]) : "";
    const county = (countyFromCol && !/^unknown$/i.test(countyFromCol))
      ? countyFromCol.replace(/\s*county$/i, "").trim()
      : (parseCountyFromDesc(desc) || "Unknown");

    const narrative = normalizeNarrativeText(extractNarrativeFromDesc(desc));
    const reopenFmt = parseReopenToMMDDYY_HHMM(end);

    const narrativeHasBetween = /\bbetween\s+Exit\b/i.test(narrative);
    const betweenText = (!narrativeHasBetween && between)
      ? ` between ${between.from} and ${between.to}.`
      : "";

    const line = `${route} (${county} County) | ${narrative}${betweenText} Estimated Reopen: ${reopenFmt}`;

    items.push({
      route,
      direction: dir,
      between,
      county,
      anticipated_end_time: end ? end : "",
      description: desc,
      formatted: line
    });
  }

  return {
    name: "major_route_closures",
    fetched_at: trafficTable.fetched_at,
    source_url: trafficTable.url,
    count: items.length,
    items
  };
}

/* ---------- LANE RESTRICTIONS ---------- */

function buildLaneRestrictionsFromTraffic(trafficTable) {
  const headers = trafficTable.headers || [];
  const rows = trafficTable.rows || [];

  const typeIdx =
    idx(headers, "Type") ??
    findHeader(headers, /type/i);

  const roadwayIdx =
    idx(headers, "Roadway") ??
    findHeader(headers, /roadway/i);

  const stateIdx =
    idx(headers, "State") ??
    findHeader(headers, /\bstate\b/i);

  const countyIdx =
    idx(headers, "County") ??
    idx(headers, "County Name") ??
    findHeader(headers, /\bcounty\b/i);

  const descIdx =
    idx(headers, "Description") ??
    findHeader(headers, /description/i);

  const startIdx =
    idx(headers, "Start Time") ??
    idx(headers, "Reported Time") ??
    findHeader(headers, /start time|reported/i);

  const endIdx =
    idx(headers, "Anticipated End Time") ??
    idx(headers, "End Time") ??
    findHeader(headers, /anticipated end|end time|\bend\b/i);

  const updatedIdx =
    idx(headers, "Last Updated") ??
    findHeader(headers, /last updated|updated/i);

  const items = [];

  for (const r of rows) {
    const type = norm(typeIdx != null ? r[typeIdx] : "");
    const roadway = norm(roadwayIdx != null ? r[roadwayIdx] : "");
    const state = norm(stateIdx != null ? r[stateIdx] : "");
    const county = norm(countyIdx != null ? r[countyIdx] : "");
    const desc = norm(descIdx != null ? r[descIdx] : "");
    const start = norm(startIdx != null ? r[startIdx] : "");
    const end = norm(endIdx != null ? r[endIdx] : "");
    const updated = norm(updatedIdx != null ? r[updatedIdx] : "");

    if (!desc) continue;

    if (!/major route/i.test(type)) continue;
    if (!/\bthere is a lane restriction\b/i.test(desc)) continue;

    const route = parseRoute(roadway || desc) || "ROUTE";
    const direction = parseDirection(desc) || parseDirection(roadway) || "";
    const countyClean = county
      ? county.replace(/\s*county$/i, "").trim()
      : (parseCountyFromDesc(desc) || "Unknown");

    let narrative = desc.replace(/\s*There is a lane restriction\.?\s*$/i, "").trim();
    if (narrative && !/[.!?]$/.test(narrative)) narrative += ".";

    const reopenFmt = parseReopenToMMDDYY_HHMM(end);
    const line = `${route} (${countyClean} County) | ${narrative} Estimated Reopen: ${reopenFmt}`;

    items.push({
      type,
      roadway,
      state,
      county: countyClean,
      route,
      direction,
      description: desc,
      start_time: start,
      anticipated_end_time: end,
      last_updated: updated,
      formatted: line
    });
  }

  return {
    name: "lane_restrictions",
    fetched_at: trafficTable.fetched_at,
    source_url: trafficTable.url,
    count: items.length,
    items
  };
}

/* ---------- MAIN ---------- */

async function main() {
  const outputs = [
    {
      name: "road_conditions",
      url: "https://www.511pa.com/list/roadcondition"
    },
    {
      name: "travel_delays",
      url: "https://www.511pa.com/list/events/traffic?start=0&length=250&order%5Bi%5D=8&order%5Bdir%5D=desc"
    },
    {
      name: "restrictions",
      url: "https://www.511pa.com/list/allrestrictioneventslist?start=0&length=100&order%5Bi%5D=4&order%5Bdir%5D=asc"
    }
  ];

  if (!fs.existsSync("data")) fs.mkdirSync("data");

  const resultsByName = {};

  for (const o of outputs) {
    const data = await scrapeSimpleTable(o.url, "table");
    resultsByName[o.name] = data;
    fs.writeFileSync(`data/${o.name}.json`, JSON.stringify(data, null, 2));
    console.log(`Wrote data/${o.name}.json (${data.rows.length} rows)`);
  }

  const major = buildMajorRouteClosures(resultsByName.travel_delays);
  fs.writeFileSync(`data/major_route_closures.json`, JSON.stringify(major, null, 2));
  console.log(`Wrote data/major_route_closures.json (${major.count} items)`);

  const lane = buildLaneRestrictionsFromTraffic(resultsByName.travel_delays);
  fs.writeFileSync(`data/lane_restrictions.json`, JSON.stringify(lane, null, 2));
  console.log(`Wrote data/lane_restrictions.json (${lane.count} items)`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
