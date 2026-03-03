import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// Tighter PA-centric framing
const VIEW = { Zoom: 8, Latitude: 40.95, Longitude: -77.75 };

function buildUrl() {
  const u = new URL("https://www.511pa.com/map");
  u.searchParams.set("Zoom", String(VIEW.Zoom));
  u.searchParams.set("Latitude", String(VIEW.Latitude));
  u.searchParams.set("Longitude", String(VIEW.Longitude));
  return u.toString();
}

// Kill onboarding / walkthrough modals so they can’t block the map
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
    selectors.forEach((sel) => {
      document.querySelectorAll(sel).forEach((el) => el.remove());
    });
    document.body.style.overflow = "auto";
  });
  await page.waitForTimeout(800);
}

// Utility: click first visible locator from a list of selectors (best-effort)
async function clickFirstVisible(page, selectors, opts = {}) {
  for (const sel of selectors) {
    const loc = page.locator(sel).first();
    try {
      if (await loc.isVisible({ timeout: opts.timeout ?? 800 })) {
        await loc.click({ timeout: opts.timeout ?? 1200 });
        if (opts.afterWaitMs) await page.waitForTimeout(opts.afterWaitMs);
        return true;
      }
    } catch {}
  }
  return false;
}

// Utility: set a checkbox within the Legend panel by its visible label text (best-effort)
async function setLegendCheckbox(page, labelText, checked) {
  // Try a few robust patterns:
  // 1) label:has-text("X") input[type=checkbox]
  // 2) row containing text then find checkbox in it
  const label = String(labelText);

  // Pattern A
  try {
    const input = page.locator(`label:has-text("${label}") input[type="checkbox"]`).first();
    if (await input.count()) {
      const isChecked = await input.isChecked().catch(() => null);
      if (isChecked !== null && isChecked !== checked) {
        await input.click({ timeout: 2000 });
        await page.waitForTimeout(400);
      }
      return true;
    }
  } catch {}

  // Pattern B
  try {
    const row = page.locator(`*:has-text("${label}")`).filter({ has: page.locator('input[type="checkbox"]') }).first();
    if (await row.count()) {
      const input = row.locator('input[type="checkbox"]').first();
      const isChecked = await input.isChecked().catch(() => null);
      if (isChecked !== null && isChecked !== checked) {
        await input.click({ timeout: 2000 });
        await page.waitForTimeout(400);
      }
      return true;
    }
  } catch {}

  return false;
}

async function openLegend(page) {
  // Legend button in top-right
  await clickFirstVisible(page, [
    'button:has-text("Legend")',
    '[aria-label="Legend"]',
    'text=Legend'
  ], { afterWaitMs: 600 });
}

async function closeWeatherRestrictionUI(page) {
  // Bottom strip: "Weather Restriction Information" has a close (X) at right in your screenshot
  // Try common close button patterns inside that container
  const bottomPanel = page.locator('text=Weather Restriction Information').first();
  try {
    if (await bottomPanel.isVisible({ timeout: 800 })) {
      // Look near it for a close button
      await clickFirstVisible(page, [
        'button[aria-label="Close"]',
        'button[title="Close"]',
        '.close',
        '.ui-dialog-titlebar-close',
        'button:has-text("×")',
        'button:has-text("X")'
      ], { afterWaitMs: 600 });
    }
  } catch {}

  // Upper-left card: "WEATHER RESTRICTION INFORMATION"
  // Some builds have a small X or collapse chevron inside that card.
  try {
    const card = page.locator('text=WEATHER RESTRICTION INFORMATION').first();
    if (await card.isVisible({ timeout: 800 })) {
      // Click any close/collapse control inside the same area
      await clickFirstVisible(page, [
        // close buttons often appear as an X icon button
        'div:has-text("WEATHER RESTRICTION INFORMATION") button[aria-label="Close"]',
        'div:has-text("WEATHER RESTRICTION INFORMATION") button[title="Close"]',
        'div:has-text("WEATHER RESTRICTION INFORMATION") .close',
        'div:has-text("WEATHER RESTRICTION INFORMATION") button:has-text("×")',
        'div:has-text("WEATHER RESTRICTION INFORMATION") button:has-text("X")',
        // collapse/chevron
        'div:has-text("WEATHER RESTRICTION INFORMATION") button[aria-label*="collapse" i]',
        'div:has-text("WEATHER RESTRICTION INFORMATION") button[aria-label*="minimize" i]'
      ], { afterWaitMs: 600 });
    }
  } catch {}
}

async function moveMyRoutesIn(page) {
  // In your screenshot there’s a round chevron/arrow button near the top bar (next to “INFO”)
  // Clicking it usually collapses/reduces the header/sidebar footprint.
  await clickFirstVisible(page, [
    // round button containing an arrow/chevron
    'button:has(svg)',
    // sometimes it’s an anchor/div with role button
    '[role="button"]:has(svg)',
    // fallback: a button near the top bar that isn’t Legend
    'button[title*="Collapse" i]',
    'button[aria-label*="Collapse" i]',
  ], { afterWaitMs: 800 });
}

async function configureLayers(page) {
  // Make sure legend is open so toggles exist
  await openLegend(page);

  // Desired: Closures -> Major Routes only
  // Remove: Incidents
  // Keep: Vehicle Restrictions (so restrictions show)
  // Optional: keep Weather Restrictions off/on? You didn’t ask to keep it. We’ll leave Weather Restrictions as-is.
  await setLegendCheckbox(page, "Incidents", false);

  // Some builds have "Closures" as a section header, with sub-items.
  // Ensure Major Routes is checked, Other Routes unchecked.
  await setLegendCheckbox(page, "Closures", true).catch?.(() => {});
  await setLegendCheckbox(page, "Major Routes", true);
  await setLegendCheckbox(page, "Other Routes", false);

  // Vehicle Restrictions ON (your screenshot shows it, keep it)
  await setLegendCheckbox(page, "Vehicle Restrictions", true);

  // If you want only major closures + restrictions, these tend to clutter:
  await setLegendCheckbox(page, "Track My Plow", false);
  await setLegendCheckbox(page, "Cameras", false);

  // Let the map refresh layers
  await page.waitForTimeout(1500);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  await killOnboarding(page);

  // Configure the view/UI
  await configureLayers(page);
  await closeWeatherRestrictionUI(page);
  await moveMyRoutesIn(page);

  // Final settle time so polylines/icons render
  await page.waitForTimeout(6000);

  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
