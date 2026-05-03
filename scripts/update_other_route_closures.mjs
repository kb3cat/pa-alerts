import fs from "fs/promises";
import { chromium } from "playwright";

const OUT_FILE = "data/other_route_closures.json";
const PAGE_SIZE = 100;

const BASE_URL = "https://www.511pa.com/list/events/traffic";

function buildUrl(start) {
  const u = new URL(BASE_URL);
  u.searchParams.set("start", String(start));
  u.searchParams.set("length", String(PAGE_SIZE));
  u.searchParams.set("filters[0][i]", "1");
  u.searchParams.set("filters[0][s]", "Closure - Other Route");
  u.searchParams.set("order[i]", "8");
  u.searchParams.set("order[dir]", "desc");
  return u.toString();
}

function norm(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

async function dismissPopups(page) {
  for (const sel of [
    'button:has-text("Done")',
    'button:has-text("Next")',
    'button[aria-label="Close"]'
  ]) {
    try {
      await page.click(sel, { timeout: 1500 });
    } catch {}
  }
}

async function scrapePage(page, start) {
  const url = buildUrl(start);
  console.log(`Loading other-route closures start=${start}`);
  console.log(url);

  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

  await dismissPopups(page);

  await page.waitForTimeout(2500);

  await page.waitForSelector("tbody tr", { timeout: 30000 }).catch(() => null);

  const rows = await page.$$eval("tbody tr", trs => {
    return trs.map(tr => {
      const cells = Array.from(tr.querySelectorAll("td")).map(td =>
        td.innerText.replace(/\s+/g, " ").trim()
      );

      if (cells.length < 8) return null;

      return {
        type: cells[0] || "",
        roadway: cells[1] || "",
        state: cells[2] || "",
        county: cells[3] || "",
        description: cells[4] || "",
        start_time: cells[5] || "",
        anticipated_end_time: cells[6] || "",
        last_updated: cells[7] || "",
        map: cells[8] || ""
      };
    }).filter(Boolean);
  });

  const filteredToType = rows.filter(r => {
  const type = (r.type || "").toLowerCase();
  const desc = (r.description || "").toLowerCase();

  return (
    type.includes("other route") ||
    desc.includes("other route")
  );
});

  console.log(`Rows found: ${rows.length}; Other Route rows: ${filteredToType.length}`);

  return filteredToType;
}

function isIncidentClosure(row) {
  const text = norm(`${row.description} ${row.roadway}`).toLowerCase();

  if (!/all lanes closed/i.test(text)) return false;

  const drop = [
    "roadwork",
    "utility work",
    "bridge outage",
    "construction",
    "maintenance",
    "paving",
    "planned",
    "line painting",
    "special event",
    "parade",
    "permit"
  ];

  const keep = [
    "crash",
    "vehicle crash",
    "multi-vehicle",
    "multi vehicle",
    "overturned",
    "jackknifed",
    "downed tree",
    "tree down",
    "downed wire",
    "downed wires",
    "wire down",
    "wires down",
    "debris on roadway",
    "flooding",
    "damaged roadway",
    "disabled vehicle",
    "police activity",
    "fire activity",
    "incident"
  ];

  if (drop.some(k => text.includes(k))) return false;
  return keep.some(k => text.includes(k));
}

function formatRow(row) {
  return `${row.description} | ${row.roadway} | ${row.county} County | Start: ${row.start_time || "TBD"} | End: ${row.anticipated_end_time || "TBD"} | Updated: ${row.last_updated || "TBD"}`;
}

function dedupeKey(row) {
  return [
    row.type,
    row.roadway,
    row.county,
    row.description,
    row.start_time,
    row.anticipated_end_time
  ].map(norm).join("|").toLowerCase();
}

async function main() {
  await fs.mkdir("data", { recursive: true });
  await fs.mkdir("debug", { recursive: true }).catch(() => {});

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    viewport: { width: 1600, height: 1000 }
  });

  const all = [];
  const seen = new Set();

  try {
    for (let start = 0; ; start += PAGE_SIZE) {
      const rows = await scrapePage(page, start);

      if (start === 0 && rows.length === 0) {
        await page.screenshot({
          path: "debug/other_route_closures_0.png",
          fullPage: true
        });
        console.log("No rows found at start=0. Debug screenshot saved to debug/other_route_closures_0.png");
      }

      for (const row of rows) {
        const key = dedupeKey(row);
        if (seen.has(key)) continue;
        seen.add(key);
        all.push(row);
      }

      if (rows.length < PAGE_SIZE) break;
    }
  } finally {
    await browser.close();
  }

  const filtered = all.filter(isIncidentClosure);

  const output = {
    updated: new Date().toISOString(),
    source: "511PA Closure - Other Route",
    total_raw: all.length,
    total_filtered: filtered.length,
    items: filtered.map(row => ({
      ...row,
      formatted: formatRow(row)
    })),
    raw_items: all.map(row => ({
      ...row,
      formatted: formatRow(row)
    }))
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(output, null, 2));

  console.log(`Wrote ${OUT_FILE}`);
  console.log(`Raw: ${all.length}`);
  console.log(`Filtered: ${filtered.length}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
