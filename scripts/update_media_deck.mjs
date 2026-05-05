import fs from "fs";
import fetch from "node-fetch";
import Parser from "rss-parser";

const parser = new Parser();
const config = JSON.parse(fs.readFileSync("data/media_sources.json", "utf-8"));

const now = Date.now();
const maxAgeMs = config.maxAgeDays * 24 * 60 * 60 * 1000;

async function fetchRSS(source) {
  try {
    const feed = await parser.parseURL(source.url);

    return (feed.items || []).map(item => ({
      title: item.title || "No title",
      link: item.link || "",
      pubDate: item.pubDate || new Date().toISOString(),
      source: source.name,
      image: item.enclosure?.url || null
    }));
  } catch (e) {
    console.log(`RSS failed: ${source.name}`);
    return [];
  }
}

async function fetchNWS(source) {
  try {
    const res = await fetch(source.url, {
      headers: { "User-Agent": "kb3cat-media-deck" }
    });

    const data = await res.json();

    return (data.features || []).map(f => ({
      title: f.properties.headline,
      link: f.properties.uri,
      pubDate: f.properties.sent,
      source: source.name,
      image: null
    }));
  } catch (e) {
    console.log(`NWS failed: ${source.name}`);
    return [];
  }
}

function fetchLocalJSON(source) {
  try {
    const data = JSON.parse(fs.readFileSync(source.url, "utf-8"));

    return data.map(item => ({
      title: item.title,
      link: item.link,
      pubDate: item.pubDate,
      source: source.name,
      image: item.image || null
    }));
  } catch (e) {
    console.log(`Local JSON failed: ${source.name}`);
    return [];
  }
}

async function build() {
  const output = {
    updated: new Date().toISOString(),
    markets: []
  };

  for (const market of config.markets) {
    let items = [];

    for (const source of market.sources) {
      let result = [];

      if (source.type === "rss") {
        result = await fetchRSS(source);
      } else if (source.type === "nws-api") {
        result = await fetchNWS(source);
      } else if (source.type === "local-json") {
        result = fetchLocalJSON(source);
      }

      items.push(...result);
    }

    items = items
      .filter(i => new Date(i.pubDate).getTime() > now - maxAgeMs)
      .sort((a, b) => new Date(b.pubDate) - new Date(a.pubDate));

    output.markets.push({
      id: market.id,
      title: market.title,
      items
    });
  }

  fs.writeFileSync("data/media_deck.json", JSON.stringify(output, null, 2));
}

build();
