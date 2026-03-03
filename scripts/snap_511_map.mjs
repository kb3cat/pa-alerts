import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";

// Tuned to keep Erie visible while still showing most of PA.
const VIEW = {
  Zoom: 7,
  Latitude: 41.05,
  Longitude: -78.85
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

  // Best-effort “cookie / onboarding / modal” removal.
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
      "#tourDialog",
      "[aria-modal='true']"
    ];
    selectors.forEach(sel =>
      document.querySelectorAll(sel).forEach(el => el.remove())
    );
    document.body.style.overflow = "auto";
  });

  // Best-effort click common accept buttons (if present, don’t fail if not)
  const acceptish = page.locator(
    'button:has-text("Accept"), button:has-text("I Agree"), button:has-text("Got it"), button:has-text("OK")'
  );
  if (await acceptish.first().isVisible({ timeout: 1500 }).catch(() => false)) {
    await acceptish.first().click({ timeout: 1500 }).catch(() => {});
  }

  await page.waitForTimeout(1000);
}

async function openLegend(page) {
  const btn = page.locator('button:has-text("Legend")').first();
  if (await btn.isVisible({ timeout: 8000 }).catch(() => false)) {
    await btn.click().catch(() => {});
    await page.waitForTimeout(1200);
    return true;
  }
  return false;
}

/**
 * Force-set a legend checkbox by label text (best effort).
 * Tries scoped legend panel first, then falls back to whole page.
 */
async function forceSetLegendCheckbox(page, labelText, checked) {
  // Attempt 1: scope to a likely legend panel
  const scoped = page.locator('div:has-text("Travel Info")').first();
  const scopes = [scoped, page.locator("body")];

  for (const scope of scopes) {
    const label = scope.locator(`label:has-text("${labelText}")`).first();
    if (!(await label.count().catch(() => 0))) continue;

    const input = label.locator('input[type="checkbox"]').first();
    if (!(await input.count().catch(() => 0))) continue;

    const cur = await input.isChecked().catch(() => null);
    if (cur !== null && cur === checked) return true;

    // Try clicking input (forced)
    try {
      await input.click({ force: true, timeout: 2500 });
      await page.waitForTimeout(500);
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
        await page.waitForTimeout(700);
        const now = await input.isChecked().catch(() => null);
        if (now === checked) return true;
      }
    } catch {}
  }

  return false;
}

async function configureLegend(page) {
  await openLegend(page);

  // EXACT settings requested:
  await forceSetLegendCheckbox(page, "Incidents", false);
  await forceSetLegendCheckbox(page, "Closures", true);
  await forceSetLegendCheckbox(page, "Major Routes", true);
  await forceSetLegendCheckbox(page, "Other Routes", false);

  await page.waitForTimeout(2000);
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
  await page.waitForTimeout(400);
}

/**
 * More reliable render readiness:
 * 1) Wait for .gm-style to exist
 * 2) Wait for some successful “tile-ish” responses
 * 3) Wait briefly for layout to settle
 * Never throws (best-effort).
 */
async function waitForMapReady(page, { minTileResponses = 8, timeoutMs = 60000 } = {}) {
  await page.waitForTimeout(1200);

  // 1) gm-style root
  await page
    .waitForFunction(() => !!document.querySelector(".gm-style"), { timeout: 30000 })
    .catch(() => {});

  // 2) tile-ish network responses
  let tileOkCount = 0;

  const isTileish = (url, headers) => {
    const u = url.toLowerCase();
    // Common Google Maps tile endpoints/hosts (varies)
    const hostHints = ["gstatic", "googleapis", "google.com", "ggpht"];
    const pathHints = ["vt?", "/vt/", "tile", "kh?", "lyrs", "mts0", "mts1", "mt0", "mt1"];
    const ct = (headers?.["content-type"] || "").toLowerCase();
    const ctOk =
      ct.includes("image/") ||
      ct.includes("application/octet-stream") ||
      ct.includes("application/x-protobuf");

    return hostHints.some(h => u.includes(h)) && pathHints.some(p => u.includes(p)) && ctOk;
  };

  const onResponse = (resp) => {
    try {
      const url = resp.url();
      const status = resp.status();
      if (status < 200 || status >= 400) return;
      const headers = resp.headers();
      if (isTileish(url, headers)) tileOkCount++;
    } catch {}
  };

  page.on("response", onResponse);

  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (tileOkCount >= minTileResponses) break;

    // Also accept DOM evidence of imagery as a secondary “ok”
    const domOk = await page
      .evaluate(() => {
        const gm = document.querySelector(".gm-style");
        if (!gm) return false;

        // loaded tile imgs
        const imgs = Array.from(gm.querySelectorAll("img"));
        const loaded = imgs.filter(i => (i.naturalWidth || 0) > 64 && (i.naturalHeight || 0) > 64);
        if (loaded.length >= 2) return true;

        // background images
        const all = Array.from(gm.querySelectorAll("*"));
        let hits = 0;
        for (const el of all) {
          const bg = (getComputedStyle(el).backgroundImage || "").toLowerCase();
          if (bg.includes("http") && (bg.includes("gstatic") || bg.includes("google"))) {
            hits++;
            if (hits >= 2) return true;
          }
        }
        return false;
      })
      .catch(() => false);

    if (domOk) break;

    await page.waitForTimeout(600);
  }

  page.off("response", onResponse);

  if (tileOkCount < minTileResponses) {
    console.warn(
      `[warn] Map readiness: only saw ${tileOkCount}/${minTileResponses} tile-ish responses before timeout; continuing.`
    );
  }

  await page.waitForTimeout(1200);
}

/**
 * Screenshot with quick sanity check (size-based) + retries.
 * This helps avoid saving a “blue/blank” frame that rendered too early.
 */
async function screenshotWithRetries(page, outPath, { attempts = 3, minBytes = 220000 } = {}) {
  const tmp = `${outPath}.tmp`;

  for (let i = 1; i <= attempts; i++) {
    // Ensure legend is open at capture time.
    await openLegend(page);

    await page.screenshot({ path: tmp, fullPage: false });

    const st = await fs.stat(tmp).catch(() => null);
    const size = st?.size || 0;

    if (size >= minBytes) {
      await fs.rename(tmp, outPath);
      return { ok: true, size, attempt: i };
    }

    console.warn(`[warn] Screenshot attempt ${i}/${attempts} looks small (${size} bytes). Retrying…`);
    await page.waitForTimeout(2000);
    await waitForMapReady(page, { minTileResponses: 6, timeoutMs: 20000 });
  }

  // Last resort: keep the last tmp even if small
  await fs.rename(tmp, outPath).catch(async () => {
    // if rename fails, try copy+unlink
    const buf = await fs.readFile(tmp);
    await fs.writeFile(outPath, buf);
    await fs.unlink(tmp).catch(() => {});
  });

  return { ok: false };
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch({
    args: [
      "--no-sandbox",
      // Help prevent headless “blue map” / WebGL issues:
      "--use-gl=swiftshader",
      "--ignore-gpu-blocklist",
      "--enable-webgl",
      "--disable-dev-shm-usage"
    ]
  });

  const context = await browser.newContext({
    viewport: { width: 1600, height: 900 },
    locale: "en-US",
    timezoneId: "America/New_York"
  });

  const page = await context.newPage();

  await page.goto(buildUrl(), {
    waitUntil: "domcontentloaded",
    timeout: 120000
  });

  // Best-effort settling (don’t fail if it never reaches idle).
  await page.waitForLoadState("networkidle", { timeout: 45000 }).catch(() => {});
  await page.waitForTimeout(1500);

  await killOnboarding(page);
  await configureLegend(page);
  await removeBottomWeatherBox(page);

  await waitForMapReady(page);

  const res = await screenshotWithRetries(page, OUT_PATH, {
    attempts: 3,
    minBytes: 220000
  });

  await browser.close();

  if (!res.ok) {
    console.warn(`[warn] Wrote ${OUT_PATH} but it may still be under-rendered. Check output.`);
  } else {
    console.log(`Wrote ${OUT_PATH} (attempt ${res.attempt}, ${res.size} bytes)`);
  }
}

main().catch(e => {
  console.error(e);
  process.exit(1);
});
