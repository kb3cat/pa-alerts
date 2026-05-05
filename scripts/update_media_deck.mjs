import fs from 'node:fs/promises';
import crypto from 'node:crypto';
import { XMLParser } from 'fast-xml-parser';

const CONFIG_PATH = 'data/media_sources.json';
const OUT_PATH = 'data/media_deck.json';
const USER_AGENT = 'kb3cat-pa-media-deck/1.0 (public safety dashboard; contact: administrator@kb3cat.com)';
const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: '@_', textNodeName: '#text' });

function arr(v) { return Array.isArray(v) ? v : (v ? [v] : []); }
function stripHtml(s='') { return String(s).replace(/<script[\s\S]*?<\/script>/gi, '').replace(/<style[\s\S]*?<\/style>/gi, '').replace(/<[^>]+>/g, ' ').replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/\s+/g, ' ').trim(); }
function firstText(v) { if (!v) return ''; if (typeof v === 'string') return v; if (typeof v === 'object') return v['#text'] || v['@_href'] || ''; return String(v); }
function linkFrom(item) {
  if (!item) return '';
  if (typeof item.link === 'string') return item.link;
  for (const l of arr(item.link)) if (l?.['@_href']) return l['@_href'];
  return item.guid?.['#text'] || item.guid || '';
}
function imageFrom(item) {
  if (!item) return '';
  const candidates = [];
  for (const k of ['media:content','media:thumbnail','enclosure']) for (const x of arr(item[k])) candidates.push(x?.['@_url']);
  const html = item.description || item['content:encoded'] || item.summary || '';
  const m = String(html).match(/<img[^>]+src=["']([^"']+)["']/i);
  if (m) candidates.push(m[1]);
  return candidates.find(Boolean) || '';
}
function hashItem(source, title, url, date) {
  return crypto.createHash('sha1').update(`${source}|${title}|${url}|${date}`).digest('hex').slice(0, 16);
}
async function fetchText(url) {
  const ctrl = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), 20000);
  try {
    const res = await fetch(url, { headers: { 'user-agent': USER_AGENT, 'accept': 'application/rss+xml, application/atom+xml, application/xml, text/xml, */*' }, signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.text();
  } finally { clearTimeout(timeout); }
}
function parseFeed(xml, source) {
  const doc = parser.parse(xml);
  const out = [];
  if (doc.rss?.channel) {
    for (const item of arr(doc.rss.channel.item)) {
      const title = stripHtml(firstText(item.title));
      const url = linkFrom(item);
      const published = item.pubDate || item.isoDate || item['dc:date'] || new Date().toISOString();
      if (!title || !url) continue;
      out.push({ id: hashItem(source.name, title, url, published), source: source.name, type: source.type || 'rss', title, url, description: stripHtml(item.description || item['content:encoded'] || '').slice(0, 260), image: imageFrom(item), published_at: new Date(published).toISOString() });
    }
  } else if (doc.feed) {
    for (const item of arr(doc.feed.entry)) {
      const title = stripHtml(firstText(item.title));
      const url = linkFrom(item);
      const published = item.updated || item.published || new Date().toISOString();
      if (!title || !url) continue;
      out.push({ id: hashItem(source.name, title, url, published), source: source.name, type: source.type || 'atom', title, url, description: stripHtml(item.summary || item.content || '').slice(0, 260), image: imageFrom(item), published_at: new Date(published).toISOString() });
    }
  }
  return out.filter(x => !Number.isNaN(new Date(x.published_at).getTime()));
}
async function main() {
  const cfg = JSON.parse(await fs.readFile(CONFIG_PATH, 'utf8'));
  const maxAgeMs = (cfg.maxAgeDays ?? 3) * 86400000;
  const cutoff = Date.now() - maxAgeMs;
  const result = { generated_at: new Date().toISOString(), max_age_days: cfg.maxAgeDays ?? 3, markets: [] };
  for (const market of cfg.markets || []) {
    const items = [];
    const errors = [];
    for (const source of market.sources || []) {
      if (!source.url || source.disabled) continue;
      try { items.push(...parseFeed(await fetchText(source.url), source)); }
      catch (e) { errors.push(`${source.name}: ${e.message}`); }
    }
    const seen = new Set();
    const filtered = items
      .filter(i => new Date(i.published_at).getTime() >= cutoff)
      .sort((a,b) => new Date(b.published_at) - new Date(a.published_at))
      .filter(i => { const key = i.id; if (seen.has(key)) return false; seen.add(key); return true; })
      .slice(0, 100);
    result.markets.push({ id: market.id, title: market.title, errors, items: filtered });
  }
  await fs.mkdir('data', { recursive: true });
  await fs.writeFile(OUT_PATH, JSON.stringify(result, null, 2));
  console.log(`Wrote ${OUT_PATH}`);
}
main().catch(err => { console.error(err); process.exit(1); });
