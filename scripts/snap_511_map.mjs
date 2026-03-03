import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

/*
  Zoom 7 keeps Erie in frame.
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
  await page.waitForTimeout(3000);

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

  await page.waitForTimeout(1000);
}

async function openLegend(page) {
  const btn = page.locator('button:has-text("Legend")').first();

  if (await btn.isVisible({ timeout: 5000 })) {
    await btn.click();
    await page.waitForTimeout(1500);
  }
}

async function configureLegend(page) {
  await openLegend(page);

  // Scope only inside legend container
  const legendPanel = page.locator('div:has-text("Travel Info")').first();

  await page.waitForTimeout(1000);

  async function toggle(labelText, shouldBeChecked) {
    const label = legendPanel.locator(`label:has-text("${labelText}")`).first();
    if (!(await label.count())) return;

    const input = label.locator('input[type="checkbox"]').first();
    if (!(await input.count())) return;

    const current = await input.isChecked().catch(() => null);
    if (current === null || current === shouldBeChecked) return;

    await label.click({ force: true });
    await page.waitForTimeout(800);
  }

  await toggle("Incidents", false);
  await toggle("Closures", true);
  await toggle("Major Routes", true);
  await toggle("Other Routes", false);

  await page.waitForTimeout(3000);
}

async function removeBottomWeatherBox(page) {
  await page.evaluate(() => {
    const elements = Array.from(document.querySelectorAll("body *"));

    for (const el of elements) {
      if (
        el.textContent &&
        el.textContent.includes("Weather Restriction Information") &&
        el.tagName !== "BODY"
      ) {
        const container = el.closest("div, section, aside, article");
        if (container) container.remove();
      }
    }
  });

  await page.waitForTimeout(500);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch({
    args: ["--no-sandbox"]
  });

  const page = await browser.newPage({
    viewport: { width: 1600, height: 900 }
  });

  await page.goto(buildUrl(), {
    waitUntil: "networkidle",
    timeout: 120000
  });

  await killOnboarding(page);

  await configureLegend(page);

  await removeBottomWeatherBox(page);

  // Let map fully render
  await page.waitForTimeout(5000);

  // Screenshot whole visible viewport (more stable than targeting #map-canvas)
  await page.screenshot({
    path: OUT_PATH,
    fullPage: false
  });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
