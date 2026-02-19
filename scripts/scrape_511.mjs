import { chromium } from "playwright";
import fs from "fs";

async function dismissPopups(page) {
  // Try multiple times because some modals have "Next" -> "Done"
  const selectors = [
    'button:has-text("Done")',
    'button:has-text("Next")',
    'button:has-text("Close")',
    'button[aria-label="Close"]',
    'button[title="Close"]',
    '.modal button:has-text("Done")',
    '.modal button:has-text("Next")',
    '.modal button:has-text("Close")'
  ];

  for (let i = 0; i < 6; i++) {
    let clicked = false;
    for (const sel of selectors) {
      try {
        await page.click(sel, { timeout: 800 });
        clicked = true;
      } catch {}
    }
    if (!clicked) break;
    await page.waitForTimeout(400);
  }

  // Also try hitting Escape a couple times
  for (let i = 0; i < 2; i++) {
    try { await page.keyboard.press("Escape"); } catch {}
    await page.waitForTimeout(200);
  }
}

async function extractTable(page, tableSelector) {
  // Returns {headers, rows}
  const headers = await page.$$eval(`${tableSelector} thead th`, ths =>
    ths.map(th => th.innerText.trim()).filter(Boolean)
  ).catch(() => []);

  const rows = await page.$$eval(`${tableSelector} tbody tr`, trs =>
    trs.map(tr => Array.from(tr.querySelectorAll("td")).map(td => td.innerText.trim()))
      .filter(r => r.some(cell => cell && cell.length))
  ).catch(() => []);

  return { headers, rows };
}

async function scrapeTable(url, tableSelector, name) {
  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  // Slightly longer timeouts for GitHub Actions + JS-heavy pages
  page.setDefaultTimeout(30000);

  await page.goto(url, { waitUntil: "domcontentloaded" });
  await dismissPopups(page);

  // Wait for *a table* to exist
  await page.waitForSelector(tableSelector, { timeout: 20000 }).catch(() => {});

  // Many 511PA lists populate rows after initial load; wait for either rows or "No data"
  // We'll attempt a few cycles.
  let headers = [];
  let rows = [];
  for (let attempt = 0; attempt < 6; attempt++) {
    await dismissPopups(page);

    // Try to wait for at least one row
    try {
      await page.waitForSelector(`${tableSelector} tbody tr`, { timeout: 4000 });
    } catch {}

    ({ headers, rows } = await extractTable(page, tableSelector));

    if (rows.length > 0) break;

    // Some pages use DataTables wrapper and the first "table" may not be the data table.
    // Try a common DataTables selector as fallback.
    if (attempt === 2) {
      ({ headers, rows } = await extractTable(page, "table.dataTable"));
      if (rows.length > 0) {
        tableSelector = "table.dataTable";
        break;
      }
    }

    await page.waitForTimeout(1500);
  }

  // If still empty, capture screenshot to debug in Actions artifacts (committed file)
  if (rows.length === 0) {
    try {
      if (!fs.existsSync("debug")) fs.mkdirSync("debug");
      await page.screenshot({ path: `debug/${name}.png`, fullPage: true });
    } catch {}
  }

  await browser.close();

  return {
    name,
    url,
    fetched_at: new Date().toISOString(),
    table_selector_used: tableSelector,
    headers,
    rows
  };
}

async function main() {
  const outputs = [
    {
      name: "road_conditions",
      url: "https://www.511pa.com/list/roadcondition",
      tableSelector: "table"
    },
    {
      name: "travel_delays",
      url: "https://www.511pa.com/list/events/traffic",
      tableSelector: "table"
    },
    {
      name: "restrictions",
      url: "https://www.511pa.com/list/allrestrictioneventslist?start=0&length=100&order%5Bi%5D=4&order%5Bdir%5D=asc",
      tableSelector: "table"
    }
  ];

  if (!fs.existsSync("data")) fs.mkdirSync("data");
  if (!fs.existsSync("debug")) fs.mkdirSync("debug");

  for (const o of outputs) {
    const data = await scrapeTable(o.url, o.tableSelector, o.name);
    fs.writeFileSync(`data/${o.name}.json`, JSON.stringify(data, null, 2));
    console.log(`Wrote data/${o.name}.json (${data.rows.length} rows)`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
