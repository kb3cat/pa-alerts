import fs from "node:fs/promises";
import { chromium } from "playwright";

const OUT = "data/psp_x.json";
const URL = "https://x.com/pastatepolice";
const MAX_POSTS = 15;

function cleanText(text = "") {
  return text.replace(/\s+/g, " ").trim();
}

async function run() {
  const browser = await chromium.launch({
    headless: true
  });

  const page = await browser.newPage({
    userAgent:
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
  });

  console.log("Loading PSP X page...");

  // 🚨 KEY CHANGE: use domcontentloaded instead of networkidle
  await page.goto(URL, {
    waitUntil: "domcontentloaded",
    timeout: 60000
  });

  // give it a few seconds to render
  await page.waitForTimeout(5000);

  let posts = [];

  try {
    await page.waitForSelector("article", { timeout: 15000 });

    posts = await page.evaluate((MAX_POSTS) => {
      const articles = [...document.querySelectorAll("article")].slice(0, MAX_POSTS);

      return articles.map((article) => {
        const text = article.innerText || "";

        const timeEl = article.querySelector("time");
        const linkEl = timeEl?.closest("a");

        const images = [...article.querySelectorAll("img")]
          .map((img) => img.src)
          .filter(
            (src) =>
              src &&
              !src.includes("profile_images") &&
              !src.includes("emoji")
          );

        return {
          source: "PA State Police (X)",
          title: text.split("\n").filter(Boolean).slice(0, 2).join(" "),
          text,
          published_at: timeEl?.getAttribute("datetime") || new Date().toISOString(),
          url: linkEl ? `https://x.com${linkEl.getAttribute("href")}` : URL,
          image: images[0] || ""
        };
      });
    }, MAX_POSTS);

  } catch (err) {
    console.log("No articles found or page structure changed.");
  }

  await browser.close();

  const cleaned = posts
    .map((p) => ({
      ...p,
      title: cleanText(p.title),
      text: cleanText(p.text)
    }))
    .filter((p) => p.text.length > 20);

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
