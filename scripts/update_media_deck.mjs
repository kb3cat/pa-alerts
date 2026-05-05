import fs from "node:fs/promises";
import crypto from "node:crypto";
import { XMLParser } from "fast-xml-parser";

const CONFIG_PATH = "data/media_sources.json";
const OUT_PATH = "data/media_deck.json";
const USER_AGENT =
  "kb3cat-pa-media-deck/1.0 (public safety dashboard; contact: administrator@kb3cat.com)";

const parser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  textNodeName: "#text"
});

function arr(v) {
  return Array.isArray(v) ? v : v ? [v] : [];
}

function decodeHTMLEntities(text = "") {
  return String(text)
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&ldquo;/g, '"')
    .replace(/&rdquo;/g, '"')
    .replace(/&lsquo;/g, "'")
    .replace(/&rsquo;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&ndash;/g, "–")
    .replace(/&mdash;/g, "—")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function stripHtml(s = "") {
  return decodeHTMLEntities(String(s))
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanText(s = "") {
  return stripHtml(s);
}

function firstText(v) {
  if (!v) return "";
  if (typeof v === "string") return v;
  if (typeof v === "object") return v["#text"] || v["@_href"] || "";
  return String(v);
}

function linkFrom(item) {
  if (!item) return "";

  if (typeof item.link === "string") return decodeHTMLEntities(item.link);

  for (const l of arr(item.link)) {
    if (l?.["@_href"]) return decodeHTMLEntities(l["@_href"]);
  }

  return decodeHTMLEntities(item.guid?.["#text"] || item.guid || "");
}

function imageFrom(item) {
  if (!item) return "";

  const candidates = [];

  for (const k of ["media:content", "media:thumbnail", "enclosure"]) {
    for (const x of arr(item[k])) {
      if (x?.["@_url"]) candidates.push(decodeHTMLEntities(x["@_url"]));
    }
  }

  const html = item.description || item["content:encoded"] || item.summary || "";
  const m = String(html).match(/<img[^>]+src=["']([^"']+)["']/i);

  if (m) candidates.push(decodeHTMLEntities(m[1]));

  return candidates.find(Boolean) || "";
}

function hashItem(source, title, url, date) {
  return crypto
    .createHash("sha1")
    .update(`${source}|${title}|${url}|${date}`)
    .digest("hex")
    .slice(0, 16);
}

async function fetchText(url) {
  const ctrl = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), 25000);

  try {
    const res = await fetch(url, {
      headers: {
        "user-agent": USER_AGENT,
        accept:
          "application/rss+xml, application/atom+xml, application/xml, text/xml, */*"
      },
      signal: ctrl.signal
    });

    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    return await res.text();
  } finally {
    clearTimeout(timeout);
  }
}

function makeItem(source, title, url, published, description, image, type) {
  const safePublished = new Date(published);
  const publishedISO = Number.isNaN(safePublished.getTime())
    ? new Date().toISOString()
    : safePublished.toISOString();

  return {
    id: hashItem(source.name, title, url, publishedISO),
    source: source.name,
    type,
    title: cleanText(title),
    url,
    description: cleanText(description).slice(0, 320),
    image,
    published_at: publishedISO
  };
}

function parseFeed(xml, source) {
  const doc = parser.parse(xml);
  const out = [];

  if (doc.rss?.channel) {
    for (const item of arr(doc.rss.channel.item)) {
      const title = cleanText(firstText(item.title));
      const url = linkFrom(item);
      const published =
        item.pubDate ||
        item.isoDate ||
        item["dc:date"] ||
        item.published ||
        item.updated ||
        new Date().toISOString();

      if (!title || !url) continue;

      out.push(
        makeItem(
          source,
          title,
          url,
          published,
          item.description || item["content:encoded"] || item.summary || "",
          imageFrom(item),
          source.type || "rss"
        )
      );
    }
  } else if (doc.feed) {
    for (const item of arr(doc.feed.entry)) {
      const title = cleanText(firstText(item.title));
      const url = linkFrom(item);
      const published =
        item.updated ||
        item.published ||
        item["dc:date"] ||
        new Date().toISOString();

      if (!title || !url) continue;

      out.push(
        makeItem(
          source,
          title,
          url,
          published,
          item.summary || item.content || item.description || "",
          imageFrom(item),
          source.type || "atom"
        )
      );
    }
  }

  return out.filter(x => !Number.isNaN(new Date(x.published_at).getTime()));
}

async function parseLocalJson(source) {
  const raw = await fs.readFile(source.url, "utf8");
  const data = JSON.parse(raw);

  return (data.items || []).map((item, idx) =>
    makeItem(
      source,
      item.title || item.text || `${source.name} item`,
      item.url || "#",
      item.published_at || data.generated_at || new Date().toISOString(),
      item.description || item.text || "",
      item.image || "",
      source.type || "local-json"
    )
  );
}

async function main() {
  const cfg = JSON.parse(await fs.readFile(CONFIG_PATH, "utf8"));
  const maxAgeMs = (cfg.maxAgeDays ?? 3) * 86400000;
  const cutoff = Date.now() - maxAgeMs;

  const result = {
    generated_at: new Date().toISOString(),
    max_age_days: cfg.maxAgeDays ?? 3,
    markets: []
  };

  for (const market of cfg.markets || []) {
    const items = [];
    const errors = [];

    for (const source of market.sources || []) {
      if (!source.url || source.disabled) continue;

      try {
        if (source.type === "link") {
          continue;
        }

        if (source.type === "local-json") {
          items.push(...(await parseLocalJson(source)));
          continue;
        }

        const xml = await fetchText(source.url);
        items.push(...parseFeed(xml, source));
      } catch (e) {
        errors.push(`${source.name}: ${e.message}`);
      }
    }

    const seen = new Set();

    const filtered = items
      .filter(i => new Date(i.published_at).getTime() >= cutoff)
      .sort((a, b) => new Date(b.published_at) - new Date(a.published_at))
      .filter(i => {
        const key = i.url || i.id || `${i.source}|${i.title}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      })
      .slice(0, 100);

    result.markets.push({
      id: market.id,
      title: market.title,
      errors,
      items: filtered
    });
  }

  await fs.mkdir("data", { recursive: true });
  await fs.writeFile(OUT_PATH, JSON.stringify(result, null, 2));

  console.log(`Wrote ${OUT_PATH}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
