import fs from "fs/promises";
import Parser from "rss-parser";
import { chromium } from "playwright";

const parser = new Parser();

function cleanTitle(t=""){
  return t
    .replace(/^\d+\s+PHOTOS?\s*\|\s*/i,"")
    .replace(/^\d+\s+HOURS?\s+AGO\s*\|\s*/i,"")
    .replace(/^\d+\s+MINUTES?\s+AGO\s*\|\s*/i,"")
    .trim();
}

async function scrape(url, name){
  const browser = await chromium.launch();
  const page = await browser.newPage();

  await page.goto(url, {timeout:60000});
  await page.waitForTimeout(4000);

  const items = await page.evaluate(() => {
    return [...document.querySelectorAll("a")]
      .map(a=>{
        const title = a.innerText.trim();
        const href = a.href;
        const img = a.querySelector("img")?.src;

        return {title, url:href, image:img};
      })
      .filter(i=>i.title.length>20)
      .slice(0,20);
  });

  await browser.close();

  return items.map(i=>({
    source:name,
    title:cleanTitle(i.title),
    url:i.url,
    image:i.image || "",
    description:"",
    published_at:new Date().toISOString()
  }));
}

async function run(){

  const data = {
    generated_at:new Date().toISOString(),
    markets:[
      {
        id:"harrisburg",
        title:"Harrisburg",
        items: await scrape("https://local21news.com/news","CBS21")
      },
      {
        id:"philadelphia",
        title:"Philadelphia",
        items: await scrape("https://www.fox29.com/news","FOX29")
      },
      {
        id:"erie",
        title:"Erie",
        items: await scrape("https://www.erienewsnow.com/","Erie News Now")
      }
    ]
  };

  await fs.writeFile("data/media_deck.json", JSON.stringify(data,null,2));
  console.log("done");
}

run();
