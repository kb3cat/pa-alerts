import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

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
  if (await btn.isVisible({ timeout: 8000 })) {
    await btn.click();
    await page.waitForTimeout(1500);
  }
}

/**
 * Force-set a legend checkbox by label text even if it’s not visible/clickable.
 */
async function forceSetLegendCheckbox(page, labelText, checked) {
  const legendScope = page.locator('div:has-text("Travel Info")').first();

  const label = legendScope.locator(`label:has-text("${labelText}")`).first();
  if (!(await label.count())) return false;

  const input = label.locator('input[type="checkbox"]').first();
  if (!(await input.count())) return false;

  const cur = await input.isChecked().catch(() => null);
  if (cur !== null && cur === checked) return true;

  // Try clicking input (forced)
  try {
    await input.click({ force: true, timeout: 2500 });
    await page.waitForTimeout(600);
    const now = await input.isChecked().catch(() => null);
    if (now === checked) return true;
  } catch {}

  // DOM fallback
  try {
    const ok = await input.evaluate((el, desired) => {
      try {
        el.checked = desired;
        el.dispatchEvent(new Event("input", { bubbles: true }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
        return true;
      } catch {
        return false;
      }
    }, checked);

    if (ok) {
      await page.waitForTimeout(800);
      const now = await input.isChecked().catch(() => null);
      if (now === checked) return true;
    }
  } catch {}

  return false;
}

async function configureLegend(page) {
  await openLegend(page);

  // EXACT settings you requested:
  await forceSetLegendCheckbox(page, "Incidents", false);
  await forceSetLegendCheckbox(page, "Closures", true);
  await forceSetLegendCheckbox(page, "Major Routes", true);
  await forceSetLegendCheckbox(page, "Other Routes", false);

  await page.waitForTimeout(2500);
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
        const container = el.closest("div, section, aside, article");
        if (container) container.remove();
      }
    }
  });
  await page.waitForTimeout(500);
}

/**
 * Robust “map is rendered” check:
 * - Google Maps often renders tiles as CSS backgrounds, not <img>.
 * - We look for gm-style and any background-image URLs OR tile-ish images.
 * - IMPORTANT: This function NEVER throws; it logs a warning and continues.
 */
async function waitForMapRendered(page) {
  // Wait for the Google Maps root container if it exists
  await page.waitForTimeout(1500);
  await page.waitForFunction(() => {
    return !!document.querySelector(".gm-style");
  }, { timeout: 30000 }).catch(() => {});

  // Now wait (best-effort) for evidence of tiles/imagery.
  const ok = await page.waitForFunction(() => {
    const gm = document.querySelector(".gm-style");
    if (!gm) return false;

    // 1) <img> based tiles (sometimes used)
    const imgs = Array.from(gm.querySelectorAll("img"));
    const imgTiles = imgs.filter(img => {
      const s = (img.getAttribute("src") || "").toLowerCase();
      return s.includes("gstatic") || s.includes("googleapis") || s.includes("google.com");
    });
    if (imgTiles.length >= 2) return true;

    // 2) CSS background tiles (very common)
    const all = Array.from(gm.querySelectorAll("*"));
    let bgHits = 0;
    for (const el of all) {
      const bg = (getComputedStyle(el).backgroundImage || "").toLowerCase();
      if (
        bg.includes("gstatic") ||
        bg.includes("googleapis") ||
        bg.includes("google.com")
      ) {
        bgHits++;
        if (bgHits >= 2) return true;
      }
    }

    // 3) As a fallback, if the map has a bunch of children, it’s likely rendered
    const childCount = gm.querySelectorAll("*").length;
    return childCount > 200;
  }, { timeout: 45000 }).then(() => true).catch(() => false);

  if (!ok) {
    console.warn("[warn] Map render check timed out; continuing with screenshot anyway.");
  }

  // Let overlays settle
  await page.waitForTimeout(1500);
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
    waitUntil: "domcontentloaded",
    timeout: 120000
  });

  // Best-effort additional settling
  await page.waitForLoadState("networkidle", { timeout: 45000 }).catch(() => {});
  await page.waitForTimeout(2000);

  await killOnboarding(page);
  await configureLegend(page);
  await removeBottomWeatherBox(page);

  // ✅ New robust wait that DOES NOT FAIL the run
  await waitForMapRendered(page);

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
