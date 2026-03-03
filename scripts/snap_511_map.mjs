import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// Layers: Major Closures + Vehicle Restrictions (active + planned)
// (Based on the 511PA embed/map params you shared.)
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

// ✅ Kill onboarding / walkthrough modals so they can’t block the map
async function killOnboarding(page) {
  // Give the site time to spawn the walkthrough
  await page.waitForTimeout(2500);

  await page.evaluate(() => {
    const selectors = [
      // jQuery UI dialog + overlay (511PA uses these for onboarding)
      ".ui-dialog",
      ".ui-widget-overlay",

      // generic modal patterns
      ".modal",
      ".modal-backdrop",
      ".dialog",
      ".overlay",

      // sometimes IDs exist depending on rollout
      "#welcomeDialog",
      "#onboardingDialog",
      "#tourDialog"
    ];

    selectors.forEach((sel) => {
      document.querySelectorAll(sel).forEach((el) => el.remove());
    });

    // Re-enable scrolling if it got locked
    document.body.style.overflow = "auto";
  });

  // Small buffer so the map can repaint without the overlay
  await page.waitForTimeout(1000);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  // Wait for the map canvas to exist/appear
  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  // Remove onboarding/walkthrough if it appears
  await killOnboarding(page);

  // If legend exists, it’s a decent sign layers are initialized (don’t fail if not)
  await page.locator("#legend").first().waitFor({ timeout: 20000 }).catch(() => {});

  // Give tiles/layers time to render
  await page.waitForTimeout(9000);

  // Screenshot ONLY the map area (no browser chrome)
  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
