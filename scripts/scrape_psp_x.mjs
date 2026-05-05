import { chromium } from "playwright";
import fs from "fs";

const URL = "https://nitter.net/PAStatePolice"; // lightweight X frontend

function cleanText(text) {
  if (!text) return "";

  return text
    .replace(/\n+/g, " ")
    .replace(/https?:\/\/\S+/g, "") // remove URLs
    .replace(/WATCH LIVE.*$/i, "")
    .replace(/Sign up.*$/i, "")
    .replace(/Join.*$/i, "")
    .replace(/\s+/g, " ")
    .trim();
}

function isMepa(text) {
  const lower = text.toLowerCase();

  return (
    lower.includes("missing endangered person advisory") ||
    lower.includes("mepa")
  );
}

(async () => {
  const browser = await chromium.launch({
    headless: true
  });

  const page = await browser.newPage();

  console.log("Loading PSP X page...");
  await page.goto(URL, { waitUntil: "domcontentloaded" });

  const posts = await page.$$eval(".timeline-item", items =>
    items.slice(0, 10).map(item => {
      const text =
        item.querySelector(".tweet-content")?.innerText || "";

      const link =
        item.querySelector("a.tweet-link")?.href || "";

      const time =
        item.querySelector("span.tweet-date a")?.title || "";

      const img =
        item.querySelector(".attachments img")?.src || "";

      return {
        rawText: text,
        url: link,
        published_at: time,
        image: img
      };
    })
  );

  const cleaned = posts
    .map(p => {
      const text = cleanText(p.rawText);

      if (!text) return null;

      const mepa = isMepa(text);

      return {
        source: "PA State Police",
        title: mepa
          ? "Missing Endangered Person Advisory (MEPA)"
          : "",
        text,
        published_at: p.published_at,
        url: p.url,
        image: p.image
      };
    })
    .filter(Boolean);

  fs.writeFileSync(
    "data/psp_x.json",
    JSON.stringify(cleaned, null, 2)
  );

  console.log(`Wrote data/psp_x.json with ${cleaned.length} posts`);

  await browser.close();
})();
