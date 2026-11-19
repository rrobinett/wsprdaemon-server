// Page logic for the public fleet board.  No DOM, no modules.
// The page inlines this file; node tests load it with require.

const ENDPOINTS = ["https://db2.wspr.live/", "https://wd1.wsprdaemon.org/", "https://wd2.wsprdaemon.org/"];
const FETCH_TIMEOUT_MS = 10000;
const STALE_AFTER_S = 900;          // a board built more than 15 minutes ago greys out
const LOW_FRACTION = 0.30;          // matches the server's LOW_FRACTION
const COLUMNS = ["STATION", "ON AIR", "DEVICES", "CLIENTS", "SPOTS vs USUAL", "CAPACITY", "DRIFT", "ALARMS"];
const SOURCE_NAMES = {wspr_noise: "wspr noise", wspr_spots: "wspr spots", ft8: "ft8", codar: "codar", hfdl: "hfdl"};
const SPOT_NAMES = {wspr_spots: "wspr", ft8: "ft8"};
const NUMERIC_TYPE = /^(Nullable\()?(U?Int\d+|Float\d+)\)?$/;

function enc(s) { return encodeURIComponent(s).replace(/%3A/g, ":"); }

function parseHash(hash) {
  const h = String(hash || "").replace(/^#/, "");
  for (const part of h.split(";")) {
    if (part.slice(0, 8) !== "station=") continue;
    try {
      const v = decodeURIComponent(part.slice(8));
      if (v) return {station: v};
    } catch (e) { /* malformed escape: ignore */ }
  }
  return {station: null};
}

function formatHash(state) {
  return state && state.station ? "#station=" + enc(state.station) : "#";
}

function parseJson(text) {
  if (text && typeof text === "object") return text;
  try {
    const v = JSON.parse(text);
    return v && typeof v === "object" ? v : {};
  } catch (e) { return {}; }
}

function sortRows(rows) {
  return rows.slice().sort((a, b) => Number(a.rank) - Number(b.rank));
}

function staleness(builtAtUtc, nowMs) {
  const b = Number(builtAtUtc);
  if (!b) return "stale";
  return nowMs / 1000 - b > STALE_AFTER_S ? "stale" : "fresh";
}

function ageText(seconds) {
  const s = Math.max(0, seconds);
  if (s < 3600) return Math.max(1, Math.round(s / 60)) + " min";
  if (s < 48 * 3600) return Math.floor(s / 3600) + " h";
  return Math.floor(s / 86400) + " d";
}

function utcText(unix) {
  const d = new Date(Number(unix) * 1000);
  if (!isFinite(d.getTime())) return "unknown time";
  return d.toISOString().slice(0, 16).replace("T", " ") + " UTC";
}

function newestArrival(sources) {
  let m = null;
  for (const k of Object.keys(sources)) {
    const t = sources[k] && sources[k].last_arrival_utc;
    if (t !== null && t !== undefined && (m === null || Number(t) > m)) m = Number(t);
  }
  return m;
}

// stale: the board is older than STALE_AFTER_S; text health (✓) must not outlive its colour.
function cellText(row, column, nowMs, stale) {
  const sources = parseJson(row.sources);
  switch (column) {
    case "STATION": return row.station + (row.grid6 ? " " + row.grid6 : "");
    case "ON AIR": {
      if (row.verdict === "grey") return "no data yet";
      if (row.verdict !== "off") return stale ? "stale" : "✓";
      const t = newestArrival(sources);
      return t === null ? "✗" : "✗ " + ageText((nowMs === undefined ? Date.now() : nowMs) / 1000 - t);
    }
    case "DEVICES":
    case "CAPACITY": return "—";
    case "CLIENTS": {
      const bad = Object.keys(sources).filter((k) => sources[k] && sources[k].expected && sources[k].state !== "ok")
        .map((k) => SOURCE_NAMES[k] || k);
      return bad.length ? bad.join(", ") : stale ? "—" : "✓";
    }
    case "SPOTS vs USUAL": {
      const v = parseJson(row.spots_vs_usual);
      const parts = Object.keys(v).filter((k) => v[k] && typeof v[k] === "object").map((k) => {
        const last = Number(v[k].last3h), usual = Number(v[k].usual3h);
        return (SPOT_NAMES[k] || k) + " " + last + " / " + usual +
          (last >= LOW_FRACTION * usual ? (stale ? "" : " ✓") : " low");
      });
      return parts.length ? parts.join("; ") : "—";
    }
    case "DRIFT": {
      if (!row.drift) return "—";
      const d = parseJson(row.drift);
      const parts = [];
      if (Number(d.n_fault)) parts.push(d.n_fault + " fault");
      if (Number(d.n_drift)) parts.push(d.n_drift + " drift");
      if (Number(d.n_reminder)) parts.push(d.n_reminder + " test ending");
      if (d.stale) parts.push("stale");
      return parts.length ? parts.join(", ") : stale ? "—" : "✓";
    }
    case "ALARMS": {
      const open = Number(row.n_open) || 0, acked = Number(row.n_acked) || 0;
      return acked ? open + " (+" + acked + " acked)" : String(open);
    }
    default: return "";
  }
}

// A live ingest_stalled alarm is about the ingest host, not a station; it becomes a banner.
function bannerText(alarms) {
  const a = (alarms || []).find((x) => x.check === "ingest_stalled" && x.state !== "closed");
  if (!a) return null;
  return a.station + " has stopped storing data since " + utcText(a.first_seen_utc) +
    "; stations below may look silent";
}

// An acknowledged alarm is still live; the mark says an admin has taken it on.
function alarmHeading(a) {
  return a.check + (a.subject ? " " + a.subject : "") +
    (a.state === "acknowledged" ? " (ACKNOWLEDGED: an admin is on it)" : " (" + a.state + ")");
}

function detailLines(numbersText) {
  const n = parseJson(numbersText);
  return Object.keys(n).sort().map((k) => k + ": " + (n[k] !== null && typeof n[k] === "object" ? JSON.stringify(n[k]) : n[k]));
}

function coerceRows(rows, meta) {
  const cols = (meta || []).filter((m) => NUMERIC_TYPE.test(m.type)).map((m) => m.name);
  for (const row of rows) {
    for (const c of cols) if (row[c] !== null && row[c] !== undefined) row[c] = Number(row[c]);
  }
  return rows;
}

function fetchOnce(fetchFn, url, timeoutMs) {
  const ctl = new AbortController();
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => { ctl.abort(); reject(new Error("timeout")); }, timeoutMs);
  });
  const work = (async () => {
    const resp = await fetchFn(url, {signal: ctl.signal});
    if (resp && resp.ok) return resp.json();
    throw new Error("error answer");
  })();
  work.catch(() => {});
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

// One table, whole, from the first endpoint that answers with data.
async function fetchTable(fetchFn, table, timeoutMs = FETCH_TIMEOUT_MS) {
  const q = "SELECT * FROM sigmondb_public." + table + " FORMAT JSON";
  let last = "no endpoint";
  for (const base of ENDPOINTS) {
    try {
      const body = await fetchOnce(fetchFn, base + "?query=" + encodeURIComponent(q), timeoutMs);
      if (!body || !Array.isArray(body.data)) { last = base + " sent no data"; continue; }
      return {rows: coerceRows(body.data, body.meta)};
    } catch (e) {
      last = base + (e.message === "timeout" ? " timed out" : e.message === "error answer" ? " sent an error" : " unreachable");
    }
  }
  throw new Error(table + ": " + last);
}

// Never returns old rows: when no endpoint answers, data is null and lastGoodMs names the
// time of the last successful load, so the page cannot keep showing stale green.
async function loadAll(fetchFn, last, nowMs, timeoutMs = FETCH_TIMEOUT_MS) {
  try {
    const [board, alarms] = await Promise.all([fetchTable(fetchFn, "fleet_board", timeoutMs),
      fetchTable(fetchFn, "fleet_alarms", timeoutMs)]);
    return {data: {board: board.rows, alarms: alarms.rows}, asOfMs: nowMs, error: null, lastGoodMs: nowMs};
  } catch (e) {
    return {data: null, asOfMs: null, error: "No public endpoint answered (" + e.message + ")",
      lastGoodMs: last && last.asOfMs ? last.asOfMs : null};
  }
}

function maxBuiltAt(rows) {
  let m = 0;
  for (const r of rows || []) { const b = Number(r.built_at_utc) || 0; if (b > m) m = b; }
  return m;
}

if (typeof module !== "undefined") {
  module.exports = {ENDPOINTS, COLUMNS, STALE_AFTER_S, sortRows, staleness, cellText, bannerText, alarmHeading, detailLines,
    parseHash, formatHash, fetchTable, loadAll, maxBuiltAt, utcText, ageText, parseJson};
}
