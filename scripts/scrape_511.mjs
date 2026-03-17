import { chromium } from "playwright";
import fs from "fs";

async function scrapeTable(url, tableSelector = "table") {
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
  const i = headers.findIndex(h => h.toLowerCase() === name.toLowerCase());
  return i >= 0 ? i : null;
}

function findHeader(headers, re) {
  const i = headers.findIndex(h => re.test(h || ""));
  return i >= 0 ? i : null;
}

function norm(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function parseRoute(desc) {
  const m = String(desc || "").match(/\b(I|US|PA)\s*[- ]?\s*(\d{1,3})\b/i);
  return m ? `${m[1].toUpperCase()}-${m[2]}` : null;
}

function parseDirection(desc) {
  const m = String(desc || "").match(/\b(NORTH|SOUTH|EAST|WEST|NB|SB|EB|WB)\b/i);
  if (!m) return "";
  const d = m[1].toUpperCase();
  if (d === "NB") return "NORTH";
  if (d === "SB") return "SOUTH";
  if (d === "EB") return "EAST";
  if (d === "WB") return "WEST";
  return d;
}

function parseCountyFromDesc(desc) {
  const m = String(desc || "").match(/\b([A-Za-z .'-]+)\s+County\b/i);
  return m ? m[1].trim() : null;
}

function parseReopen(endRaw) {
  const s = norm(endRaw);
  if (!s) return "TBD";

  const m = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4}).*?(\d{1,2}):(\d{2})\s*(AM|PM)/i);
  if (!m) return "TBD";

  let hh = parseInt(m[4], 10);
  const mm = m[5];
  const ap = m[6].toUpperCase();

  if (ap === "PM" && hh !== 12) hh += 12;
  if (ap === "AM" && hh === 12) hh = 0;

  return `${m[1]}/${m[2]}/${m[3].slice(-2)} - ${String(hh).padStart(2,"0")}:${mm}`;
}

/* ---------- LANE RESTRICTIONS ---------- */

function buildLaneRestrictionsFromTraffic(trafficTable) {
  const headers = trafficTable.headers || [];
  const rows = trafficTable.rows || [];

  const typeIdx = findHeader(headers, /type/i);
  const descIdx = findHeader(headers, /description/i);
  const countyIdx = findHeader(headers, /county/i);
  const endIdx = findHeader(headers, /end/i);

  const items = [];

  for (const r of rows) {
    const type = norm(r[typeIdx]);
    const desc = norm(r[descIdx]);
    const countyRaw = norm(r[countyIdx]);
    const end = norm(r[endIdx]);

    if (!desc) continue;

    // ONLY Major Route
    if (!/major route/i.test(type)) continue;

    // ONLY Lane Restriction wording
    if (!/\bthere is a lane restriction\b/i.test(desc)) continue;

    const route = parseRoute(desc) || "ROUTE";
    const county = countyRaw || parseCountyFromDesc(desc) || "Unknown";

    let narrative = desc.replace(/\s*There is a lane restriction\.?/i, "").trim();
    if (!/[.!?]$/.test(narrative)) narrative += ".";

    const reopen = parseReopen(end);

    items.push({
      route,
      county,
      description: desc,
      formatted: `${route} (${county} County) | ${narrative} Estimated Reopen: ${reopen}`
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
      url: "https://www.511pa.com/list/roadcondition",
    },
    {
      name: "travel_delays",
      url: "https://www.511pa.com/list/events/traffic?start=0&length=100&order%5Bi%5D=8&order%5Bdir%5D=desc",
    },
    {
      name: "restrictions",
      url: "https://www.511pa.com/list/allrestrictioneventslist?start=0&length=100",
    }
  ];

  if (!fs.existsSync("data")) fs.mkdirSync("data");

  const results = {};

  for (const o of outputs) {
    const data = await scrapeTable(o.url);
    results[o.name] = data;
    fs.writeFileSync(`data/${o.name}.json`, JSON.stringify(data, null, 2));
    console.log(`Wrote ${o.name}.json (${data.rows.length})`);
  }

  const lane = buildLaneRestrictionsFromTraffic(results.travel_delays);
  fs.writeFileSync(`data/lane_restrictions.json`, JSON.stringify(lane, null, 2));

  console.log(`Wrote lane_restrictions.json (${lane.count})`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
