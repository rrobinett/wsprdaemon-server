// Page logic for the public transmitter map.  No DOM, no modules.
// The page inlines this file; node tests load it with require.

const ENDPOINTS = ["https://db2.wspr.live/", "https://wd1.wsprdaemon.org/", "https://wd2.wsprdaemon.org/"];
const TABLES_READ = ["transmitters", "groups", "heard", "ionosphere_hourly", "receivers", "trust", "transmitter_offsets"];
const CACHE_MAX_AGE_MS = 3600000;

function enc(s) { return encodeURIComponent(s).replace(/%3A/g, ":"); }

function parseHash(hash) {
  const state = {view: "timing", sel: null, classes: null};
  let h = String(hash || "").replace(/^#/, "");
  if (!h) return state;
  const slash = h.indexOf("/");
  const view = slash < 0 ? h : h.slice(0, slash);
  if (view !== "timing" && view !== "ionosphere") return state;
  state.view = view;
  if (slash < 0) return state;
  for (const part of h.slice(slash + 1).split(";")) {
    const eq = part.indexOf("=");
    if (eq < 0) continue;
    const key = part.slice(0, eq);
    let val;
    try { val = decodeURIComponent(part.slice(eq + 1)); } catch (e) { continue; }
    if (key === "tx" || key === "group" || key === "station") {
      if (val) state.sel = {type: key, id: val};
    } else if (key === "class") {
      const cs = part.slice(eq + 1).split(",").map((c) => {
        try { return decodeURIComponent(c); } catch (e) { return ""; }
      }).filter(Boolean);
      if (cs.length) state.classes = cs;
    }
  }
  return state;
}

function formatHash(state) {
  const parts = [];
  if (state.sel) parts.push(state.sel.type + "=" + enc(state.sel.id));
  if (state.classes && state.classes.length) parts.push("class=" + state.classes.map(enc).join(","));
  return "#" + state.view + (parts.length ? "/" + parts.join(";") : "");
}

function resolve(state, data) {
  if (!state.sel) return {state, missing: false};
  const {type, id} = state.sel;
  const src = type === "tx" ? [data.transmitters, "tx_id"]
    : type === "group" ? [data.groups, "group_id"]
    : [data.receivers, "reporter_id"];
  const found = (src[0] || []).some((r) => r[src[1]] === id);
  if (found) return {state, missing: false};
  return {state: Object.assign({}, state, {sel: null}), missing: true};
}

function stageOf(targetId, data) {
  if ((data.transmitter_offsets || []).some((r) => r.tx_id === targetId)) return 4;
  const rows = (data.heard || []).filter((r) => r.target_id === targetId);
  if (rows.some((r) => r.path === "ground_range" && r.t6_7d > 0)) return 3;
  if (rows.length) return 2;
  return 1;
}

function canPin(row, trustByReporter) {
  const t = trustByReporter && trustByReporter[row.reporter_id];
  return !!t && t.trusted == 1 && row.path === "ground_range" && row.t6_7d > 0;
}

function t6Share(reporterId, heard) {
  let n = 0, t6 = 0;
  for (const r of heard || []) {
    if (r.reporter_id !== reporterId) continue;
    n += r.n_24h || 0;
    t6 += r.t6_24h || 0;
  }
  return n > 0 ? t6 / n : null;
}

function visible(state, data) {
  const transmitters = data.transmitters || [];
  const groups = data.groups || [];
  const heard = data.heard || [];
  const classOf = new Map();
  for (const t of transmitters) classOf.set(t.tx_id, t.class);
  for (const g of groups) classOf.set(g.group_id, g.class);
  const classOk = (id) => !state.classes || state.classes.includes(classOf.get(id));

  let rows = heard.filter((r) => classOk(r.target_id));
  const sel = state.sel;
  if (sel && sel.type === "station") rows = rows.filter((r) => r.reporter_id === sel.id);
  else if (sel) rows = rows.filter((r) => r.target_id === sel.id);

  let targets, reporters;
  if (!sel) {
    targets = new Set([...classOf.keys()].filter(classOk));
    reporters = new Set((data.receivers || []).map((r) => r.reporter_id));
    for (const r of rows) reporters.add(r.reporter_id);
  } else if (sel.type === "station") {
    targets = new Set(rows.map((r) => r.target_id));
    reporters = new Set([sel.id]);
  } else {
    targets = new Set([sel.id]);
    reporters = new Set(rows.map((r) => r.reporter_id));
  }
  return {targets, reporters, rows};
}

function hull(points) {
  if (points.length < 3) return points.slice();
  const pts = points.slice().sort((a, b) => a[0] - b[0] || a[1] - b[1])
    .filter((p, i, a) => i === 0 || p[0] !== a[i - 1][0] || p[1] !== a[i - 1][1]);
  if (pts.length < 3) return pts;
  const cross = (o, a, b) => (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0]);
  const build = (list) => {
    const out = [];
    for (const p of list) {
      while (out.length >= 2 && cross(out[out.length - 2], out[out.length - 1], p) <= 0) out.pop();
      out.push(p);
    }
    out.pop();
    return out;
  };
  return build(pts).concat(build(pts.slice().reverse()));
}

const FETCH_TIMEOUT_MS = 10000;
const NUMERIC_TYPE = /^(Nullable\()?(U?Int\d+|Float\d+)\)?$/;

function coerceRows(rows, meta) {
  const cols = (meta || []).filter((m) => NUMERIC_TYPE.test(m.type)).map((m) => m.name);
  if (!cols.length) return rows;
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
    if (!resp || !resp.ok) throw new Error("error answer");
    return resp.json();
  })();
  work.catch(() => {});
  return Promise.race([work, timeout]).finally(() => clearTimeout(timer));
}

async function fetchTable(fetchFn, table, timeoutMs) {
  const q = "SELECT * FROM sigmondb_public." + table + " FORMAT JSON";
  let last = "no endpoint";
  for (const base of ENDPOINTS) {
    try {
      const body = await fetchOnce(fetchFn, base + "?query=" + encodeURIComponent(q), timeoutMs);
      if (!body || !Array.isArray(body.data)) { last = base + " sent no data"; continue; }
      return coerceRows(body.data, body.meta);
    } catch (e) {
      last = base + (e.message === "timeout" ? " timed out" : " unreachable");
    }
  }
  throw new Error(table + ": " + last);
}

async function loadAll(fetchFn, cache, nowMs, timeoutMs = FETCH_TIMEOUT_MS) {
  if (cache && cache.data && nowMs - cache.asOfMs < CACHE_MAX_AGE_MS) {
    return {data: cache.data, asOfMs: cache.asOfMs, stale: false, error: null};
  }
  try {
    const rows = await Promise.all(TABLES_READ.map((t) => fetchTable(fetchFn, t, timeoutMs)));
    const data = {};
    TABLES_READ.forEach((t, i) => { data[t] = rows[i]; });
    return {data, asOfMs: nowMs, stale: false, error: null};
  } catch (e) {
    const error = "No public endpoint answered (" + e.message + ")";
    if (cache && cache.data) return {data: cache.data, asOfMs: cache.asOfMs, stale: true, error};
    return {data: null, asOfMs: null, stale: false, error};
  }
}

if (typeof module !== "undefined") {
  module.exports = {FETCH_TIMEOUT_MS, ENDPOINTS, TABLES_READ, parseHash, formatHash, resolve, stageOf, canPin,
    t6Share, visible, hull, loadAll};
}
