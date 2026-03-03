import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// Slightly tighter PA framing; tweak Zoom to 9 if you want tighter still
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
    selectors.forEach((sel) =>
      document.querySelectorAll(sel).forEach((el) => el.remove())
    );
    document.body.style.overflow = "auto";
  });
  await page.waitForTimeout(800);
}

async function openLegend(page) {
  const btn = page.locator('button:has-text("Legend")').first();
  try {
    if (await btn.isVisible({ timeout: 3000 })) {
      await btn.click({ timeout: 3000 });
      await page.waitForTimeout(1000);
    }
  } catch {}
}

/**
 * More reliable checkbox toggle:
 * - Prefer clicking the actual input
 * - If input is hidden, click the nearest visible "checkbox box" in the same row
 */
async function setLegendCheckbox(page, labelText, checked) {
  const t = String(labelText);

  // Find a row that contains the label and a checkbox input
  const row = page
    .locator(`*:has-text("${t}")`)
    .filter({ has: page.locator('input[type="checkbox"]') })
    .first();

  if (!(await row.count())) return false;

  const input = row.locator('input[type="checkbox"]').first();
  const cur = await input.isChecked().catch(() => null);
  if (cur === null) return false;

  if (cur === checked) return true;

  // Try clicking the input itself
  try {
    await input.click({ timeout: 2000, force: true });
    await page.waitForTimeout(500);
    const now = await input.isChecked().catch(() => null);
    if (now === checked) return true;
  } catch {}

  // Fallback: click the first visible element in the row that looks like the checkbox square
  // (many UI libs wrap the checkbox and hide the input)
  const boxCandidates = row.locator('input[type="checkbox"] >> xpath=..').first(); // parent
  try {
    await boxCandidates.click({ timeout: 2000, force: true });
    await page.waitForTimeout(500);
    const now = await input.isChecked().catch(() => null);
    if (now === checked) return true;
  } catch {}

  // Last resort: click the row itself
  try {
    await row.click({ timeout: 2000, force: true });
    await page.waitForTimeout(500);
    const now = await input.isChecked().catch(() => null);
    if (now === checked) return true;
  } catch {}

  return false;
}

async function configureLegend(page) {
  await openLegend(page);

  // What you want:
  // ✅ Closures (ON)
  // ✅ Major Routes (ON)
  // ❌ Incidents (OFF)
  // ❌ Other Routes (OFF)
  //
  // Do NOT touch the other layers.

  await setLegendCheckbox(page, "Incidents", false);
  await setLegendCheckbox(page, "Closures", true);
  await setLegendCheckbox(page, "Major Routes", true);
  await setLegendCheckbox(page, "Other Routes", false);

  // Give map time to redraw
  await page.waitForTimeout(2500);
}

async function hideLeftPanels(page) {
  // Guaranteed removal: hide the left "MY ROUTES/INFO" bar and the left weather info box,
  // plus the bottom weather restriction legend bar.
  await page.evaluate(() => {
    const hideByText = (needle) => {
      const els = Array.from(document.querySelectorAll("body *"));
      for (const el of els) {
        const txt = (el.textContent || "").trim();
        if (!txt) continue;
        if (txt === needle) {
          const container = el.closest("div,section,aside,header,nav,article");
          if (container) {
            container.style.display = "none";
            container.setAttribute("data-paalerts-hidden", "1");
          }
          return true;
        }
      }
      return false;
    };

    // Top-left header strip contains "MY ROUTES"
    hideByText("MY ROUTES");

    // Left info card title
    hideByText("WEATHER RESTRICTION INFORMATION");

    // Bottom bar title
    hideByText("Weather Restriction Information");
  });

  await page.waitForTimeout(500);
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  await killOnboarding(page);

  // Set layers first
  await configureLegend(page);

  // Then hide the left UI panels you don’t want in the snapshot
  await hideLeftPanels(page);

  // Final settle for symbology
  await page.waitForTimeout(4000);

  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
