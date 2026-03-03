import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

/*
  Zoom 7 keeps Erie in frame.
  Adjust later if you want tighter framing.
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
  // Give initial UI a moment to appear
  await page.waitForTimeout(3000);

  // Remove common modal/onboarding overlays (best-effort)
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

    for (const sel of selectors) {
      document.querySelectorAll(sel).forEach(el => el.remove());
    }

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
 * 1) Try click input (force)
 * 2) If that fails, set checked via DOM + dispatch events
 */
async function forceSetLegendCheckbox(page, labelText, checked) {
  // Scope to something that only exists inside the legend.
  // (We avoid broad selectors like *:has-text("Closures") which can match other UI.)
  const legendScope = page.locator('div:has-text("Travel Info")').first();

  const label = legendScope.locator(`label:has-text("${labelText}")`).first();
  if (!(await label.count())) return false;

  const input = label.locator('input[type="checkbox"]').first();
  if (!(await input.count())) return false;

  const cur = await input.isChecked().catch(() => null);
  if (cur !== null && cur === checked) return true;

  // Try clicking the checkbox directly, forced
  try {
    await input.click({ force: true, timeout: 2500 });
    await page.waitForTimeout(600);
    const now = await input.isChecked().catch(() => null);
    if (now === checked) return true;
  } catch {}

  // DOM fallback: set state + dispatch input/change
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

  // Let overlays/lines redraw
  await page.waitForTimeout(2500);
}

async function removeBottomWeatherBox(page) {
  // Remove the bottom "Weather Restriction Information" bar (best-effort)
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
 * Key fix: wait for actual map tiles to appear.
 * The “solid blue” screenshot happens when we capture before tiles render.
 */
async function waitForMapTiles(page) {
  // Some runs take longer than others; this is a visual readiness check.
  await page.waitForFunction(() => {
    const imgs = Array.from(document.querySelectorAll("img"));
    // Google tiles commonly load from googleapis/gstatic; also allow generic tile patterns.
    const tileImgs = imgs.filter(img => {
      const s = (img.getAttribute("src") || "").toLowerCase();
      return s.includes("googleapis") || s.includes("gstatic") || s.includes("google.com");
    });
    return tileImgs.length >= 6;
  }, { timeout: 30000 });

  // Extra buffer for overlays/labels to settle
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

  // Give the app time to hydrate
  await page.waitForTimeout(2000);

  await killOnboarding(page);
  await configureLegend(page);
  await removeBottomWeatherBox(page);

  // ✅ Critical: wait for tiles so we don’t capture the blue pre-render state
  await waitForMapTiles(page);

  // Screenshot viewport (stable)
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
