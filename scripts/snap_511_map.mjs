import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// Layers based on the 511PA page code you uploaded.
// This combo shows Major Closures + Vehicle Restrictions (active + planned).
const PA511_LAYERS = [
  "MajorRouteClosure",
  "IncidentClosures",
  "AllRestrictionEvents",
  "TruckRestrictions",
  "TruckRestrictionPolyline",
  "TruckRestrictionsFuture",
  "TruckRestrictionFuturePolyline"
];

// Default view (center-ish PA). Adjust if you want tighter framing.
const VIEW = { Zoom: 7, Latitude: 41.1115303, Longitude: -78.9237541 };

function buildUrl() {
  const u = new URL("https://www.511pa.com/map");
  u.searchParams.set("SelectedLayers", PA511_LAYERS.join(","));
  u.searchParams.set("Zoom", String(VIEW.Zoom));
  u.searchParams.set("Latitude", String(VIEW.Latitude));
  u.searchParams.set("Longitude", String(VIEW.Longitude));
  return u.toString();
}

async function closeOnboardingIfPresent(page) {
  // The onboarding is a modal dialog with an X in the top-right.
  // 511PA uses jQuery UI in places; the close button is commonly .ui-dialog-titlebar-close
  const closeCandidates = [
    ".ui-dialog-titlebar-close",
    "button[aria-label='Close']",
    "button[title='Close']",
    ".modal button.close",
    ".modal .close",
    "text=Next" // fallback: some versions only show a Next button
  ];

  for (const sel of closeCandidates) {
    const loc = page.locator(sel).first();
    try {
      if (await loc.isVisible({ timeout: 1500 })) {
        await loc.click({ timeout: 1500 });
        await page.waitForTimeout(500);
      }
    } catch {
      // ignore and try next selector
    }
  }

  // Some flows show multiple steps; try a couple times.
  for (let i = 0; i < 3; i++) {
    const nextBtn = page.locator("button:has-text('Next')").first();
    try {
      if (await nextBtn.isVisible({ timeout: 1200 })) {
        await nextBtn.click({ timeout: 1200 });
        await page.waitForTimeout(600);
      }
    } catch {}

    const closeBtn = page.locator(".ui-dialog-titlebar-close").first();
    try {
      if (await closeBtn.isVisible({ timeout: 1200 })) {
        await closeBtn.click({ timeout: 1200 });
        await page.waitForTimeout(600);
      }
    } catch {}
  }
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  // Let the app boot, then dismiss onboarding if it appears
  await page.waitForTimeout(2500);
  await closeOnboardingIfPresent(page);

  // Wait for the map canvas to exist
  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  // Give tiles/layers time to render after the modal closes
  await page.waitForTimeout(9000);

  // Screenshot ONLY the map area
  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
