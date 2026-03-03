import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// Keep your tighter PA framing
const VIEW = { Zoom: 8, Latitude: 40.95, Longitude: -77.75 };

function buildUrl() {
  const u = new URL("https://www.511pa.com/map");
  u.searchParams.set("Zoom", String(VIEW.Zoom));
  u.searchParams.set("Latitude", String(VIEW.Latitude));
  u.searchParams.set("Longitude", String(VIEW.Longitude));
  return u.toString();
}

async function killOnboarding(page) {
  await page.waitForTimeout(2500);
  await page.evaluate(() => {
    const selectors = [
      ".ui-dialog",
      ".ui-widget-overlay",
      ".modal",
      ".modal-backdrop",
      ".dialog",
      ".overlay"
    ];
    selectors.forEach((sel) =>
      document.querySelectorAll(sel).forEach((el) => el.remove())
    );
    document.body.style.overflow = "auto";
  });
  await page.waitForTimeout(800);
}

async function openLegend(page) {
  const btn = page.locator('button:has-text("Legend")').first();
  if (await btn.isVisible({ timeout: 3000 })) {
    await btn.click();
    await page.waitForTimeout(1000);
  }
}

async function setCheckbox(page, labelText, checked) {
  const row = page.locator(`*:has-text("${labelText}")`)
    .filter({ has: page.locator('input[type="checkbox"]') })
    .first();

  if (!(await row.count())) return false;

  const cb = row.locator('input[type="checkbox"]').first();
  const isChecked = await cb.isChecked().catch(() => null);

  if (isChecked === null) return false;

  if (isChecked !== checked) {
    try {
      await cb.click({ timeout: 2000 });
    } catch {
      await row.click({ timeout: 2000 });
    }
    await page.waitForTimeout(500);
  }

  return true;
}

async function configureLegend(page) {
  await openLegend(page);

  // EXACTLY what you asked for:
  await setCheckbox(page, "Incidents", false);
  await setCheckbox(page, "Other Routes", false);
  await setCheckbox(page, "Major Routes", true);
  await setCheckbox(page, "Closures", false);

  // Give map time to refresh
  await page.waitForTimeout(2500);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  await killOnboarding(page);

  await configureLegend(page);

  // Let layers render
  await page.waitForTimeout(5000);

  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
