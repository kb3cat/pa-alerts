function buildLaneRestrictionsFromTraffic(trafficTable) {
  const headers = trafficTable.headers || [];
  const rows = trafficTable.rows || [];

  const typeIdx =
    idx(headers, "Type") ??
    findHeader(headers, /type/i);

  const roadwayIdx =
    idx(headers, "Roadway") ??
    findHeader(headers, /roadway/i);

  const stateIdx =
    idx(headers, "State") ??
    findHeader(headers, /\bstate\b/i);

  const countyIdx =
    idx(headers, "County") ??
    idx(headers, "County Name") ??
    findHeader(headers, /\bcounty\b/i);

  const descIdx =
    idx(headers, "Description") ??
    findHeader(headers, /description/i);

  const startIdx =
    idx(headers, "Start Time") ??
    idx(headers, "Reported Time") ??
    findHeader(headers, /start time|reported/i);

  const endIdx =
    idx(headers, "Anticipated End Time") ??
    idx(headers, "End Time") ??
    findHeader(headers, /anticipated end|end time|\bend\b/i);

  const updatedIdx =
    idx(headers, "Last Updated") ??
    findHeader(headers, /last updated|updated/i);

  const items = [];

  for (const r of rows) {
    const type = norm(typeIdx != null ? r[typeIdx] : "");
    const roadway = norm(roadwayIdx != null ? r[roadwayIdx] : "");
    const state = norm(stateIdx != null ? r[stateIdx] : "");
    const county = norm(countyIdx != null ? r[countyIdx] : "");
    const desc = norm(descIdx != null ? r[descIdx] : "");
    const start = norm(startIdx != null ? r[startIdx] : "");
    const end = norm(endIdx != null ? r[endIdx] : "");
    const updated = norm(updatedIdx != null ? r[updatedIdx] : "");

    if (!desc) continue;

    // only major routes
    if (!/major route/i.test(type)) continue;

    // only explicit lane restriction wording
    if (!/\bthere is a lane restriction\b/i.test(desc)) continue;

    const route = parseRoute(roadway || desc) || "ROUTE";
    const direction = parseDirection(desc) || parseDirection(roadway) || "";
    const between = parseBetweenExits(desc);

    const countyClean = county
      ? county.replace(/\s*county$/i, "").trim()
      : (parseCountyFromDesc(desc) || "Unknown");

    const reopenFmt = parseReopenToMMDDYY_HHMM(end);

    let narrative = desc;

    // remove trailing status sentence since the whole item is already a lane restriction item
    narrative = narrative.replace(/\s*There is a lane restriction\.?\s*$/i, "").trim();

    // normalize punctuation
    if (narrative && !/[.!?]$/.test(narrative)) narrative += ".";

    const line = `${route} (${countyClean} County) | ${narrative} Estimated Reopen: ${reopenFmt}`;

    items.push({
      type,
      roadway,
      state,
      county: countyClean,
      route,
      direction,
      between,
      description: desc,
      start_time: start,
      anticipated_end_time: end,
      last_updated: updated,
      formatted: line
    });
  }

  return {
    name: "lane_restrictions",
    fetched_at: trafficTable.fetched_at,
    source_url: trafficTable.url,
    headers,
    count: items.length,
    items
  };
}
