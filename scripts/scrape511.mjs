import { chromium } from "playwright";
import fs from "fs";

async function scrapeTable(url, tableSelector) {
  const browser = await chromium.launch();
  const page = await browser.newPage();

  await page.goto(url, { waitUntil: "networkidle" });

  // Some 511PA pages have popups; try to dismiss if present
  for (const sel of ['button:has-text("Done")', 'button:has-text("Next")', 'button[aria-label="Close"]']) {
    try { await page.click(sel, { timeout: 1500 }); } catch {}
  }

  await page.waitForTimeout(1500);

  const rows = await page.$$eval(`${tableSelector} tbody tr`, trs =>
    trs.map(tr => Array.from(tr.querySelectorAll("td")).map(td => td.innerText.trim()))
  );

  const headers = await page.$$eval(`${tableSelector} thead th`, ths =>
    ths.map(th => th.innerText.trim())
  );

  await browser.close();

  return { url, fetched_at: new Date().toISOString(), headers, rows };
}

async function main() {
  const outputs = [];

  // These URLs are public list pages :contentReference[oaicite:3]{index=3}
  outputs.push({
    name: "travel_delays",
    url: "https://www.511pa.com/list/events/traffic",
    // You may need to adjust selector after first run; start by inspecting with DevTools
    tableSelector: "table"
  });

  outputs.push({
    name: "road_conditions",
    url: "https://www.511pa.com/list/roadcondition",
    tableSelector: "table"
  });

  // Vehicle restrictions area exists publicly :contentReference[oaicite:4]{index=4}
  outputs.push({
    name: "vehicle_restrictions",
    url: "https://www.511pa.com/map/page/LLWS",
    tableSelector: "table"
  });

  if (!fs.existsSync("data")) fs.mkdirSync("data");

  for (const o of outputs) {
    const data = await scrapeTable(o.url, o.tableSelector);
    fs.writeFileSync(`data/${o.name}.json`, JSON.stringify(data, null, 2));
    console.log(`Wrote data/${o.name}.json (${data.rows.length} rows)`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
