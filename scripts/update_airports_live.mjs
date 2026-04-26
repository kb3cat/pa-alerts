import fs from "node:fs";
import path from "node:path";
import { XMLParser } from "fast-xml-parser";

const OUTPUT_PATH = path.join("data", "airports_live.json");

const AIRPORTS = [
  "KPIT","KAGC","KLBE","KJST","KERI","KDUJ","KBFD","KBTP","KFKL","KIDI",
  "KMDT","KCXY","KMUI","KUNV","KAOO","KLNS","KIPT",
  "KPHL","KABE","KAVP","KHZL"
];

const AIRPORT_META = {
  KPIT:{iata:"PIT"}, KAGC:{iata:"AGC"}, KLBE:{iata:"LBE"}, KJST:{iata:"JST"},
  KERI:{iata:"ERI"}, KDUJ:{iata:"DUJ"}, KBFD:{iata:"BFD"}, KBTP:{iata:"BTP"},
  KFKL:{iata:"FKL"}, KIDI:{iata:"IDI"},
  KMDT:{iata:"MDT"}, KCXY:{iata:"CXY"}, KMUI:{iata:"MUI"}, KUNV:{iata:"SCE"},
  KAOO:{iata:"AOO"}, KLNS:{iata:"LNS"}, KIPT:{iata:"IPT"},
  KPHL:{iata:"PHL"}, KABE:{iata:"ABE"}, KAVP:{iata:"AVP"}, KHZL:{iata:"HZL"}
};

const FAA_NAS_XML_URL = "https://nasstatus.faa.gov/api/airport-status-information";
const FAA_OPS_PLAN_URL = "https://www.fly.faa.gov/adv/adv_spt";

const METAR_URL = (ids) =>
  `https://aviationweather.gov/api/data/metar?ids=${encodeURIComponent(ids.join(","))}&format=json`;

function nowIso() {
  return new Date().toISOString();
}

const STATUS_PRIORITY = [
  "Closed",
  "Ground Stop",
  "Ground Delay",
  "Arrival Delay",
  "Departure Delay",
  "Deicing",
  "Weather",
  "Other",
  "OK",
  "Unknown"
];

function betterStatus(a, b) {
  const ia = STATUS_PRIORITY.indexOf(a);
  const ib = STATUS_PRIORITY.indexOf(b);
  if (ia === -1 && ib === -1) return a;
  if (ia === -1) return b;
  if (ib === -1) return a;
  return ia <= ib ? a : b;
}

function toArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

function pickFirst(...vals) {
  for (const v of vals) {
    if (v !== undefined && v !== null && String(v).trim() !== "") return v;
  }
  return null;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "PA-Airports-Board/1.0 (GitHub Actions; kb3cat)"
    }
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.text();
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "PA-Airports-Board/1.0 (GitHub Actions; kb3cat)"
    }
  });
  if (!res.ok) throw new Error(`Fetch failed ${res.status} for ${url}`);
  return await res.json();
}

function parseNasXml(xmlText) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    allowBooleanAttributes: true
  });

  const root = parser.parse(xmlText);
  const out = {};
  for (const a of AIRPORTS) out[a] = { status: "OK", delay: "—" };

  function walk(node, trail = []) {
    if (!node) return;

    if (typeof node === "object") {
      const arpt = pickFirst(node.ARPT, node.Airport, node.airport, node.ARPT_CODE);

      if (arpt && typeof arpt === "string") {
        const code = arpt.trim().toUpperCase();
        if (AIRPORTS.includes(code)) {
          const trailStr = trail.join(" > ").toLowerCase();
          const name = String(pickFirst(node.Name, node.TYPE, node.Type, node.Event, node.Reason, "")).toLowerCase();

          let status = "Other";
          if (trailStr.includes("closure") || name.includes("closure") || name.includes("closed")) status = "Closed";
          else if (trailStr.includes("ground_stop") || trailStr.includes("ground stop") || name.includes("ground stop")) status = "Ground Stop";
          else if (trailStr.includes("ground_delay") || trailStr.includes("gdp") || name.includes("ground delay")) status = "Ground Delay";
          else if (trailStr.includes("arrival") || name.includes("arrival")) status = "Arrival Delay";
          else if (trailStr.includes("departure") || name.includes("departure")) status = "Departure Delay";
          else if (trailStr.includes("deicing") || name.includes("deicing")) status = "Deicing";
          else if (trailStr.includes("weather") || name.includes("weather")) status = "Weather";

          const delayVal = pickFirst(
            node.AvgDelay,
            node.AVG_DELAY,
            node.Delay,
            node.DELAY,
            node.Delay_Minutes,
            node.Minutes
          );

          let delay = "—";
          if (delayVal !== null && delayVal !== undefined) {
            const n = Number(String(delayVal).replace(/[^\d.]/g, ""));
            if (Number.isFinite(n) && n > 0) delay = String(Math.round(n));
          }

          out[code].status = betterStatus(out[code].status, status);
          if (delay !== "—") out[code].delay = delay;
        }
      }

      for (const [k, v] of Object.entries(node)) {
        if (k.startsWith("@_")) continue;
        const nextTrail = [...trail, k];
        if (Array.isArray(v)) v.forEach((x) => walk(x, nextTrail));
        else if (typeof v === "object") walk(v, nextTrail);
      }
    }
  }

  walk(root, []);
  return out;
}

function parseMetars(metarJson) {
  const out = {};
  for (const a of AIRPORTS) out[a] = { cat: "—", raw: "—", obsTime: null };

  for (const m of toArray(metarJson)) {
    const station = String(pickFirst(m.icaoId, m.station, m.stationId, m.id, m.icao, m.rawOb?.slice(0, 4), "") || "").toUpperCase();
    if (!AIRPORTS.includes(station)) continue;

    const cat = pickFirst(m.fltCat, m.flightCategory, m.fltcat) || "—";
    const raw = pickFirst(m.rawOb, m.raw, m.metar, m.text) || "—";
    const obsTime = pickFirst(m.obsTime, m.observationTime, m.reportTime) || null;

    out[station] = { cat, raw, obsTime };
  }

  return out;
}

function htmlToText(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/(p|div|tr|li|h1|h2|h3|pre)>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/[ \t]+\n/g, "\n");
}

function normalizeEventLine(line) {
  return String(line || "")
    .replace(/\s+/g, " ")
    .replace(/^[-•]\s*/, "")
    .trim();
}

function extractSectionLines(text, sectionName) {
  const rawLines = String(text || "")
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean);

  const upperSection = sectionName.toUpperCase();

  const startIdx = rawLines.findIndex(l =>
    l.toUpperCase().includes(upperSection)
  );

  if (startIdx === -1) return [];

  const stopHeaders = [
    "TERMINAL PLANNED",
    "EN ROUTE PLANNED",
    "ACTIVE AIRPORT EVENTS",
    "ACTIVE EN ROUTE EVENTS",
    "FORECAST EVENTS",
    "CONSTRAINTS",
    "PLANNED INITIATIVES",
    "STAFFING TRIGGER",
    "TERMINAL CONSTRAINTS",
    "EN ROUTE CONSTRAINTS"
  ];

  const sectionParts = [];

  for (let i = startIdx; i < rawLines.length; i++) {
    const line = rawLines[i];
    const upper = line.toUpperCase();

    if (
      i > startIdx &&
      stopHeaders.some(h => h !== upperSection && upper.includes(h))
    ) {
      break;
    }

    sectionParts.push(line);
  }

  let blob = sectionParts.join(" ");

  const sectionRe = new RegExp(sectionName.replace(/\s+/g, "\\s+"), "ig");

  blob = blob
    .replace(sectionRe, " ")
    .replace(/\bTime\b/gi, " ")
    .replace(/\bEvent\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (!blob) return [];

  const markerRe = /(?=(?:AFTER|UNTIL)\s+(?:\d{1,2}:\d{2}\s*(?:AM|PM)\s*(?:[A-Z]{2,4})?|\d{3,4}(?:\s*[A-Z]{2,4})?)\s*-?|\b\d{4}\s*(?:-\s*)?)/gi;

  const chunks = blob
    .split(markerRe)
    .map(x => normalizeEventLine(x))
    .filter(Boolean);

  return chunks.filter(line =>
    /^(AFTER|UNTIL|\d{4}\s*-\s*\d{4}|\d{4})\b/i.test(line) ||
    line.trim().startsWith("-")
  );
}

function classifyFlowEvent(text, section) {
  const s = String(text || "").toUpperCase();

  let code = "FLOW";
  let label = "FAA flow impact";
  let level = "yellow";

  if (s.includes("AIRPORT CLOSURE") || s.includes("CLOSED") || s.includes("CLOSURE")) {
    code = "CLOSURE";
    label = "Closure possible";
    level = s.includes("PROBABLE") || s.includes("EXPECTED") ? "orange" : "yellow";
  } else if (s.includes("GROUND STOP")) {
    code = "GS";
    label = "Ground stop possible";
    level = s.includes("PROBABLE") || s.includes("EXPECTED") ? "orange" : "yellow";
  } else if (s.includes("GROUND DELAY") || s.includes("DELAY PROGRAM") || s.includes("GDP")) {
    code = "GDP";
    label = "Ground delay possible";
    level = s.includes("PROBABLE") || s.includes("EXPECTED") ? "orange" : "yellow";
  } else if (
    s.includes("ROUTE") ||
    s.includes("SWAP") ||
    s.includes("CDRS") ||
    s.includes("REROUTE") ||
    s.includes("ARRIVAL ROUTES")
  ) {
    code = "ROUTE";
    label = "Route impact possible";
    level = "blue";
    if (s.includes("PROBABLE") || s.includes("EXPECTED")) level = "orange";
  }

  if (s.includes("ACTIVE") || s.includes("ISSUED")) level = "red";

  return { code, label, level, section };
}

function parseTimeAndEvent(line) {
  const clean = normalizeEventLine(line);

  const m = clean.match(/^((?:AFTER|UNTIL)\s+(?:\d{1,2}:\d{2}\s*(?:AM|PM)\s*(?:[A-Z]{2,4})?|\d{3,4}(?:\s*[A-Z]{2,4})?)|\d{4}\s*-\s*\d{4}|\d{4})\s*-?\s*(.+)$/i);

  if (m) {
    return {
      time: m[1].trim(),
      text: m[2].trim()
    };
  }

  return {
    time: "—",
    text: clean.replace(/^-/, "").trim()
  };
}

function airportCodesFromEvent(eventText) {
  const s = String(eventText || "").toUpperCase();
  const found = new Set();

  for (const icao of AIRPORTS) {
    const iata = AIRPORT_META[icao]?.iata;
    if (!iata) continue;

    const iataRe = new RegExp(`(^|[^A-Z0-9])${iata}([^A-Z0-9]|$)`);
    const icaoRe = new RegExp(`(^|[^A-Z0-9])${icao}([^A-Z0-9]|$)`);

    if (iataRe.test(s) || icaoRe.test(s)) found.add(icao);
  }

  return [...found];
}

function flowPriority(flow) {
  if (!flow) return 0;

  const levelScore = { gray: 1, blue: 2, yellow: 3, orange: 4, red: 5 }[flow.level] || 0;
  const codeScore = { FLOW: 1, ROUTE: 2, GDP: 3, GS: 4, CLOSURE: 5 }[flow.code] || 0;

  return (levelScore * 10) + codeScore;
}

function betterFlow(a, b) {
  if (!a) return b;
  if (!b) return a;
  return flowPriority(b) > flowPriority(a) ? b : a;
}

async function parseFaaFlow() {
  const base = {
    updatedAt: nowIso(),
    source: FAA_OPS_PLAN_URL,
    terminal: [],
    enroute: [],
    airports: {}
  };

  for (const a of AIRPORTS) base.airports[a] = null;

  let text = "";

  try {
    const html = await fetchText(FAA_OPS_PLAN_URL);
    text = htmlToText(html);
  } catch (e) {
    console.error("FAA flow fetch failed:", e.message);
    base.error = e.message;
    return base;
  }

  const terminalLines = extractSectionLines(text, "Terminal Planned");
  const enrouteLines = extractSectionLines(text, "En Route Planned");

  function parseLines(lines, section) {
    return lines.map(line => {
      const parsed = parseTimeAndEvent(line);
      const cls = classifyFlowEvent(parsed.text, section);
      const airports = airportCodesFromEvent(parsed.text);

      return {
        time: parsed.time,
        text: parsed.text,
        code: cls.code,
        label: cls.label,
        level: cls.level,
        section,
        airports
      };
    }).filter(x => x.text && x.text !== "—");
  }

  base.terminal = parseLines(terminalLines, "Terminal Planned");
  base.enroute = parseLines(enrouteLines, "En Route Planned");

  for (const ev of [...base.terminal, ...base.enroute]) {
    for (const icao of ev.airports || []) {
      const compact = {
        code: ev.code,
        level: ev.level,
        label: ev.label,
        time: ev.time,
        text: ev.text,
        section: ev.section
      };

      base.airports[icao] = betterFlow(base.airports[icao], compact);
    }
  }

  return base;
}

async function main() {
  let nasMap;

  try {
    const nasXml = await fetchText(FAA_NAS_XML_URL);
    nasMap = parseNasXml(nasXml);
  } catch (e) {
    nasMap = {};
    console.error("NAS fetch/parse failed:", e.message);
  }

  let metarMap;

  try {
    const metars = await fetchJson(METAR_URL(AIRPORTS));
    metarMap = parseMetars(metars);
  } catch (e) {
    metarMap = {};
    console.error("METAR fetch/parse failed:", e.message);
  }

  let faaFlow;

  try {
    faaFlow = await parseFaaFlow();
  } catch (e) {
    faaFlow = {
      updatedAt: nowIso(),
      source: FAA_OPS_PLAN_URL,
      terminal: [],
      enroute: [],
      airports: {},
      error: e.message
    };
    console.error("FAA flow parse failed:", e.message);
  }

  const combined = {
    generatedAt: nowIso(),
    faaFlow,
    airports: {}
  };

  for (const icao of AIRPORTS) {
    const nas = nasMap?.[icao] || { status: "Unknown", delay: "—" };
    const met = metarMap?.[icao] || { cat: "—", raw: "—", obsTime: null };
    const flow = faaFlow?.airports?.[icao] || null;

    combined.airports[icao] = {
      status: nas.status ?? "Unknown",
      delay: nas.delay ?? "—",
      metarCat: met.cat ?? "—",
      metarRaw: met.raw ?? "—",
      metarObsTime: met.obsTime ?? null,
      faaFlow: flow
    };
  }

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, JSON.stringify(combined, null, 2));

  console.log(`Wrote ${OUTPUT_PATH} at ${combined.generatedAt}`);
  console.log(`FAA Flow: terminal=${faaFlow?.terminal?.length || 0}, enroute=${faaFlow?.enroute?.length || 0}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
