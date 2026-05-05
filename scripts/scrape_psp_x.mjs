import fs from "node:fs/promises";
import { chromium } from "playwright";

const OUT = "data/psp_x.json";
const URL = "https://x.com/pastatepolice";
const MAX_POSTS = 15;

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

  return lines.join(" ").replace(/\s+/g, " ").trim();
}

function isMepa(text = "") {
  const lower = text.toLowerCase();
  return (
    lower.includes("missing endangered person advisory") ||
    lower.includes("mepa")
  );
}

async function run() {
  const browser = await chromium.launch({ headless: true });

  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
  });

  page.setDefaultTimeout(15000);

  console.log("Loading PSP X page...");

  await page.goto(URL, {
    waitUntil: "domcontentloaded",
    timeout: 30000
  });

  await page.waitForTimeout(5000);

  let rawPosts = [];

  try {
    await page.waitForSelector("article", { timeout: 15000 });

    rawPosts = await page.evaluate((MAX_POSTS) => {
      return [...document.querySelectorAll("article")]
        .slice(0, MAX_POSTS)
        .map(article => {
          const rawText = article.innerText || "";
          const timeEl = article.querySelector("time");
          const linkEl = timeEl?.closest("a");

          const images = [...article.querySelectorAll("img")]
            .map(img => img.src)
            .filter(src =>
              src &&
              !src.includes("profile_images") &&
              !src.includes("emoji") &&
              !src.includes("abs.twimg.com")
            );

          return {
            rawText,
            published_at: timeEl?.getAttribute("datetime") || new Date().toISOString(),
            url: linkEl ? `https://x.com${linkEl.getAttribute("href")}` : "https://x.com/pastatepolice",
            image: images[0] || ""
          };
        });
    }, MAX_POSTS);
  } catch (err) {
    console.log("No X articles found:", err.message);
  }

  await browser.close();

  const cleaned = rawPosts
    .map(p => {
      const text = cleanTweetText(p.rawText);
      const mepa = isMepa(text);

      return {
        source: "PA State Police",
        title: mepa ? "Missing Endangered Person Advisory (MEPA)" : "",
        text,
        published_at: p.published_at,
        url: p.url,
        image: p.image
      };
    })
    .filter(p => p.text.length > 20)
    .filter(p => !p.text.toLowerCase().includes("watch live"));

  if (cleaned.length === 0) {
    console.log("PSP X scrape returned 0 usable posts. Keeping existing psp_x.json.");
    return;
  }

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

run().catch(err => {
  console.error("PSP X scrape failed:", err.message);
  process.exit(0);
});
