import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

/*
  Zoom 7 keeps Erie comfortably in frame.
  If you want slightly tighter, try 7.5 (but 511 rounds).
*/
const VIEW = {
  Zoom: 7,
  Latitude: 40.95,
  Longitude: -77.75
};

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
      ".overlay",
      "#welcomeDialog",
      "#onboardingDialog",
      "#tourDialog"
    ];

    selectors.forEach(sel =>
      document.querySelectorAll(sel).forEach(el => el.remove())
    );

    document.body.style.overflow = "auto";
  });

  await page.waitForTimeout(800);
}

async function openLegend(page) {
  const btn = page.locator('button:has-text("Legend")').first();

  if (await btn.isVisible({ timeout: 5000 })) {
    await btn.click();
    await page.waitForTimeout(1500);
  }
}

async function setLegendCheckbox(page, labelText, checked) {
  const row = page
    .locator(`*:has-text("${labelText}")`)
    .filter({ has: page.locator('input[type="checkbox"]') })
    .first();

  if (!(await row.count())) return;

  const input = row.locator('input[type="checkbox"]').first();
  const current = await input.isChecked().catch(() => null);

  if (current === null || current === checked) return;

  await input.click({ force: true });
  await page.waitForTimeout(800);
}

async function configureLegend(page) {
  await openLegend(page);

  // Desired state:
  // Closures ON
  // Major Routes ON
  // Incidents OFF
  // Other Routes OFF

  await setLegendCheckbox(page, "Incidents", false);
  await setLegendCheckbox(page, "Closures", true);
  await setLegendCheckbox(page, "Major Routes", true);
  await setLegendCheckbox(page, "Other Routes", false);

  await page.waitForTimeout(3000);
}

async function removeBottomWeatherBox(page) {
  await page.evaluate(() => {
    const nodes = Array.from(document.querySelectorAll("body *"));
    for (const el of nodes) {
      if (
        el.textContent &&
        el.textContent.includes("Weather Restriction Information") &&
        el.tagName !== "BODY"
      ) {
        const container = el.closest("div,section,aside,article");
        if (container) container.remove();
      }
    }
  });

  await page.waitForTimeout(500);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1600, height: 900 }
  });

  await page.goto(buildUrl(), {
    waitUntil: "domcontentloaded",
    timeout: 120000
  });

  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  await killOnboarding(page);

  // Configure layers
  await configureLegend(page);

  // Remove only the bottom box (leave legend intact)
  await removeBottomWeatherBox(page);

  // Let map settle
  await page.waitForTimeout(4000);

  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
