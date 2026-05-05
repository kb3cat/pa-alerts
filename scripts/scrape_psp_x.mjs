import fs from "node:fs/promises";
import { chromium } from "playwright";

const OUT = "data/psp_x.json";
const URL = "https://x.com/pastatepolice";
const MAX_POSTS = 15;

function cleanText(text = "") {
  return String(text)
    .replace(/\s+/g, " ")
    .replace(/^PA State Police\s*@PAStatePolice\s*/i, "")
    .replace(/^PA State Police\s*/i, "")
    .trim();
}

function cleanTweetText(raw = "") {
  let lines = String(raw)
    .split("\n")
    .map(l => l.trim())
    .filter(Boolean);

  lines = lines.filter(l => {
    const lower = l.toLowerCase();

    if (lower === "pa state police") return false;
    if (lower === "@pastatepolice") return false;
    if (lower === "pa state police @pastatepolice") return false;
    if (lower === "reposted") return false;
    if (lower === "show more") return false;
    if (/^\d+[smhd]$/.test(lower)) return false;
    if (/^\d+(\.\d+)?[kKmM]?$/.test(lower)) return false;
    if (["reply", "repost", "like", "view"].includes(lower)) return false;

    return true;
  });

  return cleanText(lines.join(" "));
}

async function run() {
  const browser = await chromium.launch({ headless: true });

  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
  });

  console.log("Loading PSP X page...");

  await page.goto(URL, {
    waitUntil: "domcontentloaded",
    timeout: 60000
  });

  await page.waitForTimeout(5000);

  let posts = [];

  try {
    await page.waitForSelector("article", { timeout: 15000 });

    posts = await page.evaluate((MAX_POSTS) => {
      const articles = [...document.querySelectorAll("article")].slice(0, MAX_POSTS);

      return articles.map((article) => {
        const rawText = article.innerText || "";

        const timeEl = article.querySelector("time");
        const linkEl = timeEl?.closest("a");

        const images = [...article.querySelectorAll("img")]
          .map((img) => img.src)
          .filter(
            (src) =>
              src &&
              !src.includes("profile_images") &&
              !src.includes("emoji") &&
              !src.includes("abs.twimg.com")
          );

        return {
          source: "PA State Police",
          rawText,
          published_at: timeEl?.getAttribute("datetime") || new Date().toISOString(),
          url: linkEl ? `https://x.com${linkEl.getAttribute("href")}` : "https://x.com/pastatepolice",
          image: images[0] || ""
        };
      });
    }, MAX_POSTS);
  } catch {
    console.log("No articles found or page structure changed.");
  }

  await browser.close();

  const cleaned = posts
    .map((p) => {
      const text = cleanTweetText(p.rawText);
      return {
        source: "PA State Police",
        title: text.slice(0, 120),
        text,
        published_at: p.published_at,
        url: p.url,
        image: p.image
      };
    })
    .filter((p) => p.text.length > 20)
    .filter((p) => !p.text.toLowerCase().includes("watch live"));

  await fs.mkdir("data", { recursive: true });

  await fs.writeFile(
    OUT,
    JSON.stringify(
      {
        generated_at: new Date().toISOString(),
        warning: "Experimental X scrape — may fail or return incomplete data.",
        count: cleaned.length,
        items: cleaned
      },
      null,
      2
    )
  );

  console.log(`Wrote ${OUT} with ${cleaned.length} posts`);
}

run().catch((err) => {
  console.error("PSP X scrape failed:", err);
  process.exit(1);
});
