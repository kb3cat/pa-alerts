import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// tighter PA framing
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
      ".overlay",
      "#welcomeDialog",
      "#onboardingDialog",
      "#tourDialog"
    ];
    selectors.forEach((sel) => document.querySelectorAll(sel).forEach((el) => el.remove()));
    document.body.style.overflow = "auto";
  });
  await page.waitForTimeout(800);
}

// Click a checkbox by the visible text near it (robust-ish)
async function setCheckboxByText(page, text, checked) {
  const t = String(text);

  // Find a container that has the text AND a checkbox input inside it
  const row = page.locator(`*:has-text("${t}")`).filter({
    has: page.locator('input[type="checkbox"]')
  }).first();

  if (!(await row.count())) return false;

  const cb = row.locator('input[type="checkbox"]').first();
  const cur = await cb.isChecked().catch(() => null);
  if (cur === null) return false;

  if (cur !== checked) {
    // Clicking the input itself can fail if it’s hidden; click the row instead
    try { await cb.click({ timeout: 1500 }); }
    catch { await row.click({ timeout: 1500 }); }
    await page.waitForTimeout(500);
  }
  return true;
}

async function openLegend(page) {
  // Your screenshot shows a Legend button top-right
  const btn = page.locator('button:has-text("Legend")').first();
  try {
    if (await btn.isVisible({ timeout: 2000 })) {
      await btn.click({ timeout: 2000 });
      await page.waitForTimeout(800);
      return true;
    }
  } catch {}
  return false;
}

async function configureLegend(page) {
  await openLegend(page);

  // Set exactly what you requested:
  // - remove Incidents
  // - Closures with only Major Routes selected (so Other Routes off)
  // - keep Vehicle Restrictions on
  await setCheckboxByText(page, "Incidents", false);
  await setCheckboxByText(page, "Other Routes", false);
  await setCheckboxByText(page, "Major Routes", true);
  await setCheckboxByText(page, "Vehicle Restrictions", true);

  // Optional cleanup: turn off Track My Plow if it’s on
  await setCheckboxByText(page, "Track My Plow", false);

  // Give the map time to redraw
  await page.waitForTimeout(2000);
}

// Hard-hide UI overlays so the screenshot is clean and “non-clickable”
async function hideUiOverlays(page) {
  await page.evaluate(() => {
    const killByText = (needle) => {
      const nodes = Array.from(document.querySelectorAll("body *"));
      for (const el of nodes) {
        if (!el || !el.textContent) continue;
        if (el.textContent.trim() === needle) {
          // remove the nearest reasonable container
          el.closest("div,section,aside,article")?.remove();
          return true;
        }
      }
      return false;
    };

    // Remove the upper-left info card
    // (it contains this exact heading in your screenshot)
    killByText("WEATHER RESTRICTION INFORMATION");

    // Remove bottom bar by its title
    killByText("Weather Restriction Information");

    // Remove/Hide the right legend pane so it doesn’t appear in the snapshot
    // Many builds use an aside/drawer; safest is to hide anything containing "Travel Info" and checkboxes.
    const candidates = Array.from(document.querySelectorAll("aside, .drawer, .panel, .legend, div"))
      .filter(el => el && /Travel Info/i.test(el.textContent || "") && (el.querySelector('input[type="checkbox"]')));

    candidates.forEach(el => { el.style.display = "none"; });

    // Also hide the top-left header strip if it’s taking too much space
    // (This keeps “Map only” for a board look)
    const topBarCandidates = Array.from(document.querySelectorAll("header, .topbar, .appbar, .toolbar, .navbar"));
    topBarCandidates.forEach(el => {
      if (/MY ROUTES/i.test(el.textContent || "")) el.style.display = "none";
    });
  });

  await page.waitForTimeout(800);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  await killOnboarding(page);

  // Set layers first (while the UI exists)
  await configureLegend(page);

  // Then hide overlays for a clean snapshot
  await hideUiOverlays(page);

  // Final settle for tiles/overlays
  await page.waitForTimeout(4000);

  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
