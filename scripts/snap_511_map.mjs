import { chromium } from "playwright";
import fs from "fs/promises";

const OUT_PATH = "data/pa511_map.png";
const VIEW = { Zoom: 7, Latitude: 41.1115303, Longitude: -78.9237541 };

function buildUrl() {
  const u = new URL("https://www.511pa.com/map");
  u.searchParams.set("Zoom", String(VIEW.Zoom));
  u.searchParams.set("Latitude", String(VIEW.Latitude));
  u.searchParams.set("Longitude", String(VIEW.Longitude));
  return u.toString();
}

// ✅ Kill onboarding / walkthrough modals so they can’t block the map
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

/**
 * Try to enable layers using whatever map/app objects exist on the page.
 * This is defensive: it tries multiple strategies and won’t crash the run.
 */
async function enableDesiredLayers(page) {
  // Give the app time to initialize its state/store
  await page.waitForTimeout(2500);

  const result = await page.evaluate(() => {
    const wantedMatchers = [
      // major closures
      (name) => /major/i.test(name) && /closure/i.test(name),
      // closures layer often called "Major Routes" or similar
      (name) => /major/i.test(name) && /route/i.test(name) && /closure/i.test(name),
      // vehicle restrictions
      (name) => /vehicle/i.test(name) && /restriction/i.test(name),
      (name) => /truck/i.test(name) && /restriction/i.test(name),
      (name) => /restriction/i.test(name) && /event/i.test(name),
    ];

    function norm(x){ return String(x || "").trim(); }

    // Strategy A: if there is a global "store" (redux-ish), try to find layer list
    // Common patterns: window.store, window.__store__, window.appStore
    const stores = [window.store, window.__store__, window.appStore].filter(Boolean);

    // helper to toggle layers if store has expected methods
    const toggled = [];
    const seen = new Set();

    function matchesWanted(displayName){
      const n = norm(displayName);
      if (!n) return false;
      return wantedMatchers.some(fn => {
        try { return fn(n); } catch { return false; }
      });
    }

    // Try a variety of known-ish state shapes
    function tryToggleFromState(state, dispatchFn){
      if (!state) return false;

      // Look for any array of layer-like objects
      const candidates = [];
      const queue = [{ path: "root", obj: state }];
      const visited = new Set();

      while (queue.length) {
        const { path, obj } = queue.shift();
        if (!obj || typeof obj !== "object") continue;
        if (visited.has(obj)) continue;
        visited.add(obj);

        if (Array.isArray(obj) && obj.length && typeof obj[0] === "object") {
          // layer arrays are often full of objects with id/name/visible
          const keys = Object.keys(obj[0] || {});
          if (keys.some(k => /name|title|label/i.test(k)) && keys.some(k => /id|key/i.test(k))) {
            candidates.push({ path, arr: obj });
          }
        }

        // walk shallowly to avoid giant graphs
        for (const k of Object.keys(obj)) {
          const v = obj[k];
          if (v && typeof v === "object") queue.push({ path: path + "." + k, obj: v });
        }
      }

      // From candidates, attempt to toggle those that match
      let did = false;

      for (const c of candidates) {
        for (const layer of c.arr) {
          const name =
            layer.name ?? layer.title ?? layer.label ?? layer.displayName ?? layer.text ?? "";
          const id =
            layer.id ?? layer.key ?? layer.layerId ?? layer.value ?? layer.code ?? name;

          if (!matchesWanted(name)) continue;

          const layerKey = norm(id);
          if (!layerKey || seen.has(layerKey)) continue;
          seen.add(layerKey);

          // Try common dispatch actions
          const actions = [
            { type: "TOGGLE_LAYER", payload: layerKey },
            { type: "SET_LAYER_VISIBILITY", payload: { id: layerKey, visible: true } },
            { type: "LAYER_SET_VISIBLE", payload: { id: layerKey, visible: true } },
            { type: "layers/setVisible", payload: { id: layerKey, visible: true } },
          ];

          for (const a of actions) {
            try {
              if (typeof dispatchFn === "function") dispatchFn(a);
              did = true;
            } catch {}
          }

          toggled.push({ name: norm(name), id: layerKey });
        }
      }
      return did;
    }

    // Attempt store dispatch
    let didSomething = false;
    for (const st of stores) {
      try {
        const state = typeof st.getState === "function" ? st.getState() : (st.state || st.getState?.());
        const dispatchFn = typeof st.dispatch === "function" ? st.dispatch.bind(st) : null;
        if (tryToggleFromState(state, dispatchFn)) didSomething = true;
      } catch {}
    }

    // Strategy B: localStorage flags (some apps persist selected layers)
    // We’ll set a generic “selectedLayers” if it exists.
    try {
      const keys = Object.keys(localStorage || {});
      const selKey = keys.find(k => /selectedlayers/i.test(k));
      if (selKey) {
        // If there is already a value, keep it and just return info
        // (We avoid guessing layer IDs here.)
        didSomething = didSomething || true;
      }
    } catch {}

    return { didSomething, toggled };
  });

  // Give the map time to redraw after toggling
  await page.waitForTimeout(5000);

  return result;
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const browser = await chromium.launch();
  const page = await browser.newPage({ viewport: { width: 1600, height: 900 } });

  await page.goto(buildUrl(), { waitUntil: "domcontentloaded", timeout: 120000 });

  // Map canvas
  const map = page.locator("#map-canvas");
  await map.waitFor({ state: "visible", timeout: 60000 });

  await killOnboarding(page);

  // Try to enable desired layers programmatically (more reliable than URL params)
  const info = await enableDesiredLayers(page);
  console.log("Layer enable attempt:", JSON.stringify(info));

  // Extra settle time for tiles + overlays
  await page.waitForTimeout(6000);

  await map.screenshot({ path: OUT_PATH });

  await browser.close();
  console.log(`Wrote ${OUT_PATH}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
