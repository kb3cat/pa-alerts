import { chromium } from "playwright";
import fs from "fs";

async function scrapeTable(url, tableSelector = "table") {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  await page.goto(url, { waitUntil: "networkidle" });

  // Some 511PA pages have popups; try to dismiss if present
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

function idx(headers, name) {
  const i = headers.findIndex(h => h.trim().toLowerCase() === name.trim().toLowerCase());
  return i >= 0 ? i : null;
}

function norm(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function parseRoute(desc) {
  // tries to find I-##, US-##, PA-##
  const m = desc.match(/\b(I|US|PA)\s*[- ]\s*(\d{1,3})\b/i);
  if (!m) return null;
  return `${m[1].toUpperCase()}-${m[2]}`;
}

function parseDirection(desc) {
  // prefer explicit NB/SB/EB/WB
  const m1 = desc.match(/\b(NB|SB|EB|WB)\b/i);
  if (m1) return m1[1].toUpperCase();

  // fallback: NORTH/SOUTH/EAST/WEST (or “NORTHBOUND”)
  const m2 = desc.match(/\b(NORTH|SOUTH|EAST|WEST)(BOUND)?\b/i);
  if (!m2) return null;
  const dir = m2[1].toUpperCase();
  return dir; // return "NORTH", etc.
}

function parseBetweenExits(desc) {
  // handles patterns like:
  // "between Exit 100: PA 443 - PINE GROVE and Exit 138: PA 309 - MCADOO/TAMAQUA"
  // also: "between Exit: abc (##) to xyz (##)" (numbers in parens)
  const d = desc;

  // Pattern A: Exit 100: NAME ... and Exit 138: NAME ...
  const mA = d.match(/between\s+Exit\s+(\d+)\s*:\s*([^]+?)\s+(?:and|to)\s+Exit\s+(\d+)\s*:\s*([^]+?)(?:\.|$)/i);
  if (mA) {
    return {
      from: `Exit ${mA[1]}: ${norm(mA[2])}`,
      to: `Exit ${mA[3]}: ${norm(mA[4])}`
    };
  }

  // Pattern B: between Exit: abc (##) to xyz (##)
  const mB = d.match(/between\s+Exit\s*:?\s*([^()]+?)\s*\((\d+)\)\s*(?:and|to)\s*([^()]+?)\s*\((\d+)\)/i);
  if (mB) {
    return {
      from: `${norm(mB[1])} (${mB[2]})`,
      to: `${norm(mB[3])} (${mB[4]})`
    };
  }

  return null;
}

function isConstructionRelated(desc) {
  // be aggressive — you asked to ignore roadwork/construction closures
  return /(roadwork|construction|work zone|lane closure for work|paving|bridge|maintenance|utility work|shoulder work)/i.test(desc);
}

function isAllLanesClosed(desc) {
  return /\ball lanes closed\b/i.test(desc);
}

function isAllLanesOpen(desc) {
  return /\ball lanes open\b/i.test(desc);
}

function parseCountyFromDesc(desc) {
  const parts = String(desc || "")
    .split("|")
    .map(s => s.trim())
    .filter(Boolean);

  const pIdx = parts.findIndex(p => /^pennsylvania$/i.test(p));
  if (pIdx >= 0 && parts[pIdx + 1]) return parts[pIdx + 1];

  // fallback: try "X County"
  const m = String(desc || "").match(/\b([A-Za-z .'-]+)\s+County\b/i);
  if (m) return norm(m[1]);

  return null;
}

function extractNarrativeFromDesc(desc) {
  const raw = String(desc || "");
  if (!raw.includes("|")) return norm(raw);

  const parts = raw
    .split("|")
    .map(s => s.trim())
    .filter(Boolean);

  // Remove timestamp-like tokens (e.g., "2/23/26, 12:55 AM")
  const nonTime = parts.filter(p => !/^\d{1,2}\/\d{1,2}\/\d{2,4}\s*,\s*\d{1,2}:\d{2}\s*(AM|PM)$/i.test(p));

  // Remove leading metadata tokens commonly present in 511 pipe-strings
  // Examples: "Closure - Major Route", "I-80", "Pennsylvania", "Monroe"
  const cleaned = nonTime.filter(p => {
    if (/^closure\b/i.test(p)) return false;
    if (/^restriction\b/i.test(p)) return false;
    if (/^event\b/i.test(p)) return false;
    if (/^major route$/i.test(p)) return false;
    if (/^pennsylvania$/i.test(p)) return false;
    if (parseRoute(p)) return false; // route token like "I-80"
    return true;
  });

  // After stripping, the first remaining token is typically the narrative sentence
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
  return `${mo}/${da}/${yy} - ${hh2}${mm}`;
}

function normalizeNarrativeText(s) {
  return String(s || "")
    .replace(/\bMulti\s+vehicle\b/gi, "Multi-vehicle")
    .replace(/\bAll\s+lanes\s+closed\b/gi, "All lanes Closed");
}

function buildMajorRouteClosures(trafficTable) {
  const headers = trafficTable.headers || [];
  const rows = trafficTable.rows || [];

  // Try to find columns by name (best case)
  const typeI = idx(headers, "Type");
  const descI = idx(headers, "Description");
  const endI  = idx(headers, "Anticipated End Time");

  // If 511 changes header names slightly, fallback by fuzzy includes
  const typeIdx = typeI ?? headers.findIndex(h => /type/i.test(h));
  const descIdx = descI ?? headers.findIndex(h => /description/i.test(h));
  const endIdx  = endI  ?? headers.findIndex(h => /(anticipated|end time)/i.test(h));

  const items = [];

  for (const r of rows) {
    const type = norm(r[typeIdx]);
    const desc = norm(r[descIdx]);
    const end  = norm(r[endIdx]);

    if (!type || !desc) continue;

    // must be Major Route
    if (!/^major route$/i.test(type)) continue;

    // ignore roadwork/construction-related
    if (isConstructionRelated(desc)) continue;

    // if it explicitly says all lanes open, do not include
    if (isAllLanesOpen(desc)) continue;

    // only include all lanes closed
    if (!isAllLanesClosed(desc)) continue;

    const route = parseRoute(desc) || "ROUTE";
    const dir = parseDirection(desc) || "DIRECTION";
    const between = parseBetweenExits(desc);

    const etaText = end ? end : "TBD";
    const county = parseCountyFromDesc(desc) || "Unknown";
    const narrative = normalizeNarrativeText(extractNarrativeFromDesc(desc));
    const reopenFmt = parseReopenToMMDDYY_HHMM(end);

    const line = `${route} (${county} County) | ${narrative} Estimated Reopen: ${reopenFmt}`;

    items.push({
      route,
      direction: dir,
      between,
      anticipated_end_time: etaText,
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

async function main() {
  const outputs = [];

  outputs.push({
    name: "road_conditions",
    url: "https://www.511pa.com/list/roadcondition",
    tableSelector: "table"
  });

  outputs.push({
    name: "travel_delays",
    url: "https://www.511pa.com/list/events/traffic",
    tableSelector: "table"
  });

  outputs.push({
    name: "restrictions",
    url: "https://www.511pa.com/list/allrestrictioneventslist?start=0&length=100&order%5Bi%5D=4&order%5Bdir%5D=asc",
    tableSelector: "table"
  });

  if (!fs.existsSync("data")) fs.mkdirSync("data");

  const resultsByName = {};

  for (const o of outputs) {
    const data = await scrapeTable(o.url, o.tableSelector);
    resultsByName[o.name] = data;
    fs.writeFileSync(`data/${o.name}.json`, JSON.stringify(data, null, 2));
    console.log(`Wrote data/${o.name}.json (${data.rows.length} rows)`);
  }

  // Derived file: major-route, non-construction, all-lanes-closed
  const major = buildMajorRouteClosures(resultsByName.travel_delays);
  fs.writeFileSync(`data/major_route_closures.json`, JSON.stringify(major, null, 2));
  console.log(`Wrote data/major_route_closures.json (${major.count} items)`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
