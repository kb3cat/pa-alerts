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
  if (await btn.isVisible({ timeout: 5000 })) {
    await btn.click();
    await page.waitForTimeout(1500);
  }
}

/**
 * Force-set a legend checkbox by LABEL text, even if not visible.
 * Strategy:
 *  1) Find label in legend panel
 *  2) Try input.click({force:true})
 *  3) If that fails, set input.checked in DOM + dispatch change
 */
async function forceSetLegendCheckbox(page, labelText, checked) {
  // Legend panel: scope by visible section header text
  const legendPanel = page.locator('div:has-text("Travel Info")').first();

  // Find the label and the input inside it
  const label = legendPanel.locator(`label:has-text("${labelText}")`).first();
  if (!(await label.count())) return false;

  const input = label.locator('input[type="checkbox"]').first();
  if (!(await input.count())) return false;

  // If playwright can read the state, avoid unnecessary changes
  const cur = await input.isChecked().catch(() => null);
  if (cur !== null && cur === checked) return true;

  // Try clicking the checkbox directly, forced
  try {
    await input.click({ force: true, timeout: 2000 });
    await page.waitForTimeout(600);
    const now = await input.isChecked().catch(() => null);
    if (now === checked) return true;
  } catch {}

  // Last resort: set via DOM + dispatch change
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

  // EXACTLY what you want:
  // - Incidents OFF
  // - Closures ON
  // - Major Routes ON
  // - Other Routes OFF
  await forceSetLegendCheckbox(page, "Incidents", false);
  await forceSetLegendCheckbox(page, "Closures", true);
  await forceSetLegendCheckbox(page, "Major Routes", true);
  await forceSetLegendCheckbox(page, "Other Routes", false);

  // Give map time to redraw
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

  // Keep legend visible, just remove bottom info bar
  await removeBottomWeatherBox(page);

  // Let layers render
  await page.waitForTimeout(5000);

  // Screenshot the viewport (stable)
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
