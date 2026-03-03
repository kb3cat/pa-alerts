import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

/**
 * Features you said you want:
 * - Major Closures (Major Routes)
 * - Vehicle Restrictions (active + planned)
 *
 * If you later want Speed Restrictions too, add:
 * "SpeedRestrictions","SpeedRestrictionPolyline"
 */
const PA511_LAYERS = [
  "MajorRouteClosure",
  "TruckRestrictions",
  "TruckRestrictionPolyline",
  "TruckRestrictionsFuture",
  "TruckRestrictionFuturePolyline"
];

// Default view (center-ish PA). Adjust if you want tighter framing.
const VIEW = { Zoom: 8, Latitude: 41.1115303, Longitude: -78.9237541 };

function buildUrl(){
  const u = new URL("https://www.511pa.com/map");
  u.searchParams.set("SelectedLayers", PA511_LAYERS.join(","));
  u.searchParams.set("Zoom", String(VIEW.Zoom));
  u.searchParams.set("Latitude", String(VIEW.Latitude));
  u.searchParams.set("Longitude", String(VIEW.Longitude));
  return u.toString();
}

async function main(){
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({
    viewport: { width: 1600, height: 900 }
  });

  const url = buildUrl();
  await page.goto(url, { waitUntil: "networkidle", timeout: 120000 });

  // Give the map time to finish drawing layers
  await page.waitForTimeout(9000);

  // Screenshot the visible map viewport
  await page.screenshot({ path: OUT_PATH, fullPage: false });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
