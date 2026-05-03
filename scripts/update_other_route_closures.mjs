import fs from "fs/promises";
import { chromium } from "playwright";

const OUT_FILE = "data/other_route_closures.json";
const PAGE_SIZE = 100;

const BASE =
  "https://www.511pa.com/list/events/traffic";

function buildUrl(start) {
  const u = new URL(BASE);
  u.searchParams.set("start", String(start));
  u.searchParams.set("length", String(PAGE_SIZE));
  u.searchParams.set("filters[0][i]", "1");
  u.searchParams.set("filters[0][s]", "Closure - Other Route");
  u.searchParams.set("order[i]", "8");
  u.searchParams.set("order[dir]", "desc");
  return u.toString();
}

function isIncidentClosure(row) {
  const text = `${row.description || ""} ${row.roadway || ""}`.toLowerCase();

  const drop = [
    "roadwork",
    "utility work",
    "bridge outage",
    "construction",
    "maintenance",
    "paving",
    "planned"
  ];

  const keep = [
    "crash",
    "downed tree",
    "tree down",
    "wires down",
    "wire down",
    "debris on roadway",
    "flooding",
    "damaged roadway",
    "police activity",
    "fire activity",
    "incident"
  ];

  if (!text.includes("all lanes closed")) return false;
  if (drop.some(k => text.includes(k))) return false;
  return keep.some(k => text.includes(k));
}

function formatRow(row) {
  return `${row.description} | ${row.roadway} | ${row.county} | Start: ${row.startTime} | End: ${row.anticipatedEndTime} | Updated: ${row.lastUpdated}`;
}

async function scrapePage(page, start) {
  const url = buildUrl(start);
  console.log(`Loading ${url}`);

  await page.goto(url, { waitUntil: "networkidle", timeout: 60000 });

  await page.waitForSelector("table tbody tr", { timeout: 30000 }).catch(() => null);

  const rows = await page.$$eval("table tbody tr", trs => {
    return trs.map(tr => {
      const cells = [...tr.querySelectorAll("td")].map(td =>
        td.innerText.replace(/\s+/g, " ").trim()
      );

      if (cells.length < 8) return null;

      return {
        type: cells[0] || "",
        roadway: cells[1] || "",
        state: cells[2] || "",
        county: cells[3] || "",
        description: cells[4] || "",
        startTime: cells[5] || "",
        anticipatedEndTime: cells[6] || "",
        lastUpdated: cells[7] || "",
        map: cells[8] || ""
      };
    }).filter(Boolean);
  });

  return rows.filter(r => r.type === "Closure - Other Route");
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  const all = [];
  const seen = new Set();

  try {
    for (let start = 0; ; start += PAGE_SIZE) {
      const rows = await scrapePage(page, start);

      console.log(`Fetched ${rows.length} rows at start=${start}`);

      for (const row of rows) {
        const key = [
          row.type,
          row.roadway,
          row.county,
          row.description,
          row.startTime,
          row.anticipatedEndTime
        ].join("|").toLowerCase();

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
    raw_items: all
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(output, null, 2));
  console.log(`Wrote ${OUT_FILE}`);
  console.log(`Raw: ${all.length}, Filtered: ${filtered.length}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
