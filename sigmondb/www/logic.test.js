const test = require("node:test");
const assert = require("node:assert/strict");
const L = require("./logic.js");

const H = (o) => Object.assign({reporter_id: "R1", target_id: "hfrnet:PREY", target_type: "tx",
  path: "ground_range", n_24h: 0, t6_24h: 0, n_7d: 0, t6_7d: 0}, o);

test("parseHash default", () => {
  const want = {view: "timing", sel: null, classes: null};
  assert.deepEqual(L.parseHash(""), want);
  assert.deepEqual(L.parseHash("#"), want);
  assert.deepEqual(L.parseHash("#nonsense"), want);
});

test("parseHash/formatHash round-trip", () => {
  const cases = [
    ["#timing/station=KFS-MAIN", {view: "timing", sel: {type: "station", id: "KFS-MAIN"}, classes: null}],
    ["#ionosphere/tx=hfrnet:PREY", {view: "ionosphere", sel: {type: "tx", id: "hfrnet:PREY"}, classes: null}],
    ["#timing/group=4.4505%20-25.73%20E;class=ocean%20radar",
      {view: "timing", sel: {type: "group", id: "4.4505 -25.73 E"}, classes: ["ocean radar"]}],
  ];
  for (const [h, st] of cases) {
    assert.deepEqual(L.parseHash(h), st);
    assert.equal(L.formatHash(st), h);
    assert.deepEqual(L.parseHash(L.formatHash(st)), st);
  }
});

test("parseHash keeps unknown id; resolve drops it", () => {
  const st = L.parseHash("#timing/tx=hfrnet:GONE");
  assert.deepEqual(st.sel, {type: "tx", id: "hfrnet:GONE"});
  const data = {transmitters: [{tx_id: "hfrnet:PREY"}], groups: [], receivers: []};
  const r = L.resolve(st, data);
  assert.equal(r.missing, true);
  assert.equal(r.state.sel, null);
  assert.equal(r.state.view, "timing");
  const ok = L.resolve(L.parseHash("#timing/tx=hfrnet:PREY"), data);
  assert.equal(ok.missing, false);
  assert.equal(ok.state.sel.id, "hfrnet:PREY");
});

test("stageOf 1..4", () => {
  const base = {heard: [], transmitter_offsets: []};
  assert.equal(L.stageOf("a", base), 1);
  assert.equal(L.stageOf("a", {heard: [H({target_id: "a", path: "skywave", t6_7d: 5})], transmitter_offsets: []}), 2);
  assert.equal(L.stageOf("a", {heard: [H({target_id: "a", t6_7d: 0})], transmitter_offsets: []}), 2);
  assert.equal(L.stageOf("a", {heard: [H({target_id: "a", t6_7d: 3})], transmitter_offsets: []}), 3);
  assert.equal(L.stageOf("a", {heard: [H({target_id: "a", t6_7d: 3})], transmitter_offsets: [{tx_id: "a"}]}), 4);
});

test("canPin needs trust, ground range and T6", () => {
  const trust = {R1: {trusted: 1}, R2: {trusted: 0}};
  assert.equal(L.canPin(H({t6_7d: 1}), trust), true);
  assert.equal(L.canPin(H({reporter_id: "R2", t6_7d: 1}), trust), false);
  assert.equal(L.canPin(H({path: "skywave", t6_7d: 1}), trust), false);
  assert.equal(L.canPin(H({t6_7d: 0}), trust), false);
  assert.equal(L.canPin(H({reporter_id: "R9", t6_7d: 1}), trust), false);
});

test("t6Share null without arrivals", () => {
  assert.equal(L.t6Share("R1", []), null);
  assert.equal(L.t6Share("R1", [H({n_24h: 0})]), null);
  assert.equal(L.t6Share("R1", [H({n_24h: 6, t6_24h: 3}), H({n_24h: 4, t6_24h: 1}), H({reporter_id: "R2", n_24h: 90, t6_24h: 90})]), 0.4);
});

const fixture = () => ({
  transmitters: [{tx_id: "t1", class: "ocean radar"}, {tx_id: "t2", class: "other"}],
  groups: [{group_id: "g1", class: "ocean radar"}],
  receivers: [{reporter_id: "R1"}, {reporter_id: "R2"}],
  heard: [
    H({reporter_id: "R1", target_id: "t1"}),
    H({reporter_id: "R1", target_id: "g1", target_type: "group"}),
    H({reporter_id: "R2", target_id: "t2"}),
  ],
});

test("visible station selection", () => {
  const d = fixture();
  const all = L.visible({view: "timing", sel: null, classes: null}, d);
  assert.equal(all.rows.length, 3);
  assert.deepEqual([...all.targets].sort(), ["g1", "t1", "t2"]);
  assert.deepEqual([...all.reporters].sort(), ["R1", "R2"]);
  const v = L.visible({view: "timing", sel: {type: "station", id: "R1"}, classes: null}, d);
  assert.equal(v.rows.length, 2);
  assert.deepEqual([...v.targets].sort(), ["g1", "t1"]);
  assert.deepEqual([...v.reporters], ["R1"]);
  const t = L.visible({view: "timing", sel: {type: "tx", id: "t2"}, classes: null}, d);
  assert.deepEqual([...t.reporters], ["R2"]);
  const c = L.visible({view: "timing", sel: null, classes: ["ocean radar"]}, d);
  assert.deepEqual([...c.targets].sort(), ["g1", "t1"]);
  assert.equal(c.rows.length, 2);
});

test("visible ignores unknown reporter", () => {
  const d = fixture();
  d.heard.push(H({reporter_id: "NOPOS", target_id: "t1"}));
  const v = L.visible({view: "timing", sel: null, classes: null}, d);
  assert.equal(v.rows.length, 4);
  const s = L.visible({view: "timing", sel: {type: "tx", id: "t1"}, classes: null}, d);
  assert.equal(s.rows.length, 2);
  assert.ok(s.reporters.has("NOPOS"));
});

test("hull square plus centre returns the 4 corners", () => {
  const h = L.hull([[0, 0], [0, 2], [2, 2], [2, 0], [1, 1]]);
  assert.equal(h.length, 4);
  assert.deepEqual(h.map(String).sort(), ["0,0", "0,2", "2,0", "2,2"]);
  assert.deepEqual(L.hull([[1, 1]]), [[1, 1]]);
  assert.deepEqual(L.hull([[1, 1], [2, 2]]), [[1, 1], [2, 2]]);
});

const okResp = (rows) => Promise.resolve({ok: true, json: () => Promise.resolve({data: rows})});

test("query falls back and keeps the last answer", async () => {
  const urls = [];
  const f1 = (url) => {
    urls.push(url);
    if (url.startsWith("https://db2.wspr.live/")) return Promise.reject(new Error("down"));
    return okResp([{x: 1}]);
  };
  const r = await L.loadAll(f1, null, 1000);
  assert.equal(r.stale, false);
  assert.equal(r.error, null);
  assert.equal(r.asOfMs, 1000);
  for (const t of L.TABLES_READ) assert.deepEqual(r.data[t], [{x: 1}]);
  assert.ok(urls.some((u) => u.startsWith("https://wd1.wsprdaemon.org/?query=" +
    encodeURIComponent("SELECT * FROM sigmondb_public.heard FORMAT JSON"))));
  assert.ok(!urls.some((u) => u.includes("wd30")));

  const cache = {data: r.data, asOfMs: 1000};
  let calls = 0;
  const dead = () => { calls++; return Promise.resolve({ok: false}); };
  const fresh = await L.loadAll(dead, cache, 1000 + 3599999);
  assert.equal(calls, 0);
  assert.equal(fresh.stale, false);
  assert.equal(fresh.data, r.data);
  const old = await L.loadAll(dead, cache, 1000 + 3600000);
  assert.equal(old.stale, true);
  assert.equal(old.data, r.data);
  assert.equal(old.asOfMs, 1000);
  assert.ok(old.error);
  const none = await L.loadAll(dead, null, 5000);
  assert.equal(none.data, null);
  assert.ok(none.error);
});

test("loadAll coerces quoted numeric columns", async () => {
  const f = () => Promise.resolve({ok: true, json: () => Promise.resolve({
    meta: [{name: "reporter_id", type: "String"}, {name: "n_24h", type: "UInt64"},
      {name: "t6_24h", type: "Nullable(Int64)"}, {name: "x", type: "Float64"}],
    data: [{reporter_id: "R1", n_24h: "6", t6_24h: "3", x: "1.5"}, {reporter_id: "R1", n_24h: "4", t6_24h: null, x: null}],
  })});
  const r = await L.loadAll(f, null, 0);
  const row = r.data.heard[0];
  assert.equal(row.n_24h, 6);
  assert.equal(row.x, 1.5);
  assert.equal(r.data.heard[1].t6_24h, null);
  assert.equal(row.reporter_id, "R1");
  assert.equal(L.t6Share("R1", r.data.heard), 0.3);
});

test("loadAll times out a silent endpoint and falls through", async () => {
  assert.equal(L.FETCH_TIMEOUT_MS, 10000);
  let signalled = false;
  const f = (url, opts) => {
    if (url.startsWith("https://db2.wspr.live/")) {
      opts.signal.addEventListener("abort", () => { signalled = true; });
      return new Promise(() => {});
    }
    return okResp([{x: 1}]);
  };
  const r = await L.loadAll(f, null, 0, 20);
  assert.equal(r.error, null);
  assert.deepEqual(r.data.heard, [{x: 1}]);
  assert.equal(signalled, true);
});
