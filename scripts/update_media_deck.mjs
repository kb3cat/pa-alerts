import fs from "node:fs/promises";
import crypto from "node:crypto";
import Parser from "rss-parser";
import { chromium } from "playwright";

const CONFIG_PATH = "data/media_sources.json";
const OUT_PATH = "data/media_deck.json";

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36";

const parser = new Parser({
  headers: { "User-Agent": USER_AGENT },
  timeout: 25000
});

function cleanText(text = "") {
  return String(text)
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&nbsp;/g, " ")
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<style[\s\S]*?<\/style>/gi, "")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function hashItem(source, title, url, date) {
  return crypto
    .createHash("sha1")
    .update(`${source}|${title}|${url}|${date}`)
    .digest("hex")
    .slice(0, 16);
}

function normalizeDate(value) {
  const d = new Date(value || Date.now());
  return Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
}

function makeItem(sourceName, title, url, published, description = "", image = "", type = "item") {
  const publishedISO = normalizeDate(published);

  // Important: preserve intentionally blank titles, especially PSP non-MEPA posts.
  const safeTitle = title === "" ? "" : cleanText(title || "Untitled");

  return {
    id: hashItem(sourceName, safeTitle || description || url || sourceName, url || "", publishedISO),
    source: sourceName,
    type,
    title: safeTitle,
    url: url || "#",
    description: cleanText(description).slice(0, 320),
    image: image || "",
    published_at: publishedISO
  };
}

async function fetchRss(source) {
  const feed = await parser.parseURL(source.url);

  return (feed.items || []).map(item => {
    const image =
      item.enclosure?.url ||
      item["media:content"]?.url ||
      item["media:thumbnail"]?.url ||
      "";

    return makeItem(
      source.name,
      item.title,
      item.link || item.guid || source.url,
      item.isoDate || item.pubDate || item.created || item.updated,
      item.contentSnippet || item.content || item.summary || "",
      image,
      "rss"
    );
  });
}

async function fetchLocalJson(source) {
  const raw = await fs.readFile(source.url, "utf8");
  const data = JSON.parse(raw);
  const items = Array.isArray(data) ? data : (data.items || []);

  return items.map(item => {
    // Important: if title exists and is blank, keep it blank.
    const title = Object.prototype.hasOwnProperty.call(item, "title")
      ? item.title
      : "";

    return makeItem(
      item.source || source.name,
      title,
      item.url || item.link || source.url,
      item.published_at || item.pubDate || data.generated_at || new Date().toISOString(),
      item.description || item.text || "",
      item.image || "",
      "local-json"
    );
  });
}

async function scrapeSource(browser, source) {
  const page = await browser.newPage({
    userAgent: USER_AGENT,
    viewport: { width: 1366, height: 1000 }
  });

  try {
    await page.goto(source.url, {
      waitUntil: "domcontentloaded",
      timeout: 45000
    });

    await page.waitForTimeout(4500);

    const origin = new URL(source.url).origin;
    const hostname = new URL(source.url).hostname.replace(/^www\./, "");

    const items = await page.evaluate(({ sourceName, sourceUrl, origin, hostname }) => {
      function clean(s) {
        return String(s || "").replace(/\s+/g, " ").trim();
      }

      function absUrl(href) {
        try {
          return new URL(href, origin).href;
        } catch {
          return "";
        }
      }

      function isRealArticle(url, title) {
        const u = url.toLowerCase();
        const t = title.toLowerCase();

        if (t.includes("sign in") || t.includes("newsletter") || t.includes("create account")) return false;
        if (u.includes("/weather") || u.includes("/sports") || u.includes("/watch")) return false;
        if (u.includes("/contact") || u.includes("/about") || u.includes("/privacy")) return false;
        if (u.includes("/search") || u.includes("/category/") || u.includes("/features")) return false;

        if (hostname.includes("wfmz.com")) {
          return u.includes("/news/") && u.endsWith(".html");
        }

        if (hostname.includes("erienewsnow.com")) {
          return u.includes("/story/");
        }

        if (hostname.includes("local21news.com") || hostname.includes("wjactv.com")) {
          return u.includes("/news/") && !u.endsWith("/news/local");
        }

        if (hostname.includes("fox29.com")) {
          return u.includes("/news/");
        }

        if (hostname.includes("goerie.com")) {
          return u.includes("/story/");
        }

        if (hostname.includes("altoonamirror.com")) {
          return u.includes("/news/") && !u.endsWith("/news/");
        }

        return u.includes("/news/") || u.includes("/article/") || u.includes("/story/");
      }

      function bestImageFrom(el) {
        const scope =
          el.closest("article, .card, .story, .article, .tease, .content-item, .promo") || el;

        const img = scope.querySelector("img") || el.querySelector("img");
        if (!img) return "";

        const candidates = [
          img.currentSrc,
          img.src,
          img.getAttribute("data-src"),
          img.getAttribute("data-lazy-src"),
          img.getAttribute("data-original"),
          img.getAttribute("data-url")
        ].filter(Boolean);

        const srcset = img.getAttribute("srcset") || img.getAttribute("data-srcset") || "";
        if (srcset) {
          const best = srcset
            .split(",")
            .map(x => x.trim().split(" ")[0])
            .filter(Boolean)
            .pop();

          if (best) candidates.unshift(best);
        }

        for (const c of candidates) {
          const url = absUrl(c);

          if (
            url &&
            !url.includes("logo") &&
            !url.includes("sprite") &&
            !url.includes("icon") &&
            !url.includes("avatar") &&
            !url.includes("profile") &&
            !url.includes("placeholder")
          ) {
            return url;
          }
        }

        return "";
      }

      const anchors = [...document.querySelectorAll("a[href]")];

      const raw = anchors.map(a => {
        const title =
          clean(a.querySelector("h1,h2,h3,h4")?.innerText) ||
          clean(a.getAttribute("aria-label")) ||
          clean(a.innerText);

        const href = absUrl(a.getAttribute("href"));

        const article =
          a.closest("article, .card, .story, .article, .tease, .content-item, .promo") ||
          a.parentElement;

        const description = clean(
          article?.querySelector("p, .summary, .description, .dek, .teaser, .excerpt")?.innerText || ""
        );

        const timeEl = article?.querySelector("time");
        const published = timeEl?.getAttribute("datetime") || timeEl?.innerText || "";

        return {
          source: sourceName,
          title,
          url: href,
          description,
          image: bestImageFrom(a),
          published_at: published
        };
      });

      const seen = new Set();

      return raw
        .filter(item => item.title && item.url)
        .filter(item => item.url.startsWith(origin))
        .filter(item => item.url !== sourceUrl)
        .filter(item => item.title.length >= 22)
        .filter(item => item.title.length <= 180)
        .filter(item => isRealArticle(item.url, item.title))
        .filter(item => {
          const key = item.url;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        })
        .slice(0, 25);
    }, { sourceName: source.name, sourceUrl: source.url, origin, hostname });

    return items.map(item =>
      makeItem(
        source.name,
        item.title,
        item.url,
        item.published_at || new Date().toISOString(),
        item.description || "",
        item.image || "",
        "scrape"
      )
    );
  } finally {
    await page.close();
  }
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

  let browser = null;

  if ((cfg.markets || []).some(m => (m.sources || []).some(s => s.type === "scrape"))) {
    browser = await chromium.launch({ headless: true });
  }

  try {
    for (const market of cfg.markets || []) {
      const items = [];

      for (const source of market.sources || []) {
        if (!source.url || source.disabled) continue;

        try {
          if (source.type === "rss") {
            items.push(...await fetchRss(source));
          } else if (source.type === "local-json") {
            items.push(...await fetchLocalJson(source));
          } else if (source.type === "scrape") {
            items.push(...await scrapeSource(browser, source));
          }
        } catch (err) {
          console.log(`${source.type || "source"} failed: ${source.name} — ${err.message}`);
        }
      }

      const seen = new Set();

      const filtered = items
        .filter(i => new Date(i.published_at).getTime() >= cutoff)
        .sort((a, b) => new Date(b.published_at) - new Date(a.published_at))
        .filter(i => {
          const key = i.url || i.id || `${i.source}|${i.title}|${i.description}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        })
        .slice(0, 120);

      result.markets.push({
        id: market.id,
        title: market.title,
        errors: [],
        items: filtered
      });
    }
  } finally {
    if (browser) await browser.close();
  }

  await fs.mkdir("data", { recursive: true });
  await fs.writeFile(OUT_PATH, JSON.stringify(result, null, 2));

  console.log(`Wrote ${OUT_PATH}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
