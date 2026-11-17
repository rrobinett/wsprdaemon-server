const test = require("node:test");
const assert = require("node:assert/strict");
const L = require("./logic.js");

const NOW = Date.UTC(2026, 10, 17, 12, 0, 0);
const row = (o) => Object.assign({station: "AC0G-B4", grid6: "EN16ab", verdict: "ok",
  sources: JSON.stringify({wspr_noise: {expected: true, last_arrival_utc: NOW / 1000 - 60, state: "ok"}}),
  spots_vs_usual: "{}", n_open: 0, rank: 5, built_at_utc: NOW / 1000}, o);

test("stale board renders grey: 16 min stale, 14 min fresh", () => {
  assert.equal(L.staleness(NOW / 1000 - 16 * 60, NOW), "stale");
  assert.equal(L.staleness(NOW / 1000 - 14 * 60, NOW), "fresh");
});

test("staleness boundary: exactly 900 s is fresh, 901 s is stale", () => {
  assert.equal(L.staleness(NOW / 1000 - 900, NOW), "fresh");
  assert.equal(L.staleness(NOW / 1000 - 901, NOW), "stale");
});

test("staleness: a missing build time is stale", () => {
  assert.equal(L.staleness(0, NOW), "stale");
  assert.equal(L.staleness(null, NOW), "stale");
});

test("sortRows keeps the server's rank", () => {
  const rows = [row({station: "C", rank: 2}), row({station: "A", rank: 0}), row({station: "B", rank: "1"})];
  assert.deepEqual(L.sortRows(rows).map((r) => r.station), ["A", "B", "C"]);
  assert.equal(rows[0].station, "C", "input untouched");
});

test('cellText shows "—" for DEVICES and CAPACITY', () => {
  assert.equal(L.cellText(row({}), "DEVICES"), "—");
  assert.equal(L.cellText(row({}), "CAPACITY"), "—");
});

test("cellText ON AIR: ok, off with age, grey", () => {
  assert.equal(L.cellText(row({}), "ON AIR", NOW), "✓");
  const old = JSON.stringify({wspr_noise: {expected: true, last_arrival_utc: NOW / 1000 - 3 * 3600 - 120, state: "quiet"},
    ft8: {expected: true, last_arrival_utc: NOW / 1000 - 7200, state: "quiet"}});
  assert.equal(L.cellText(row({verdict: "off", sources: old}), "ON AIR", NOW), "✗ 2 h");
});

test("cellText ON AIR on a grey row says no data yet, never a tick", () => {
  const g = row({verdict: "grey", sources: JSON.stringify({wspr_noise: {expected: false, last_arrival_utc: null, state: "-"}})});
  assert.equal(L.cellText(g, "ON AIR", NOW), "no data yet");
});

test("cellText CLIENTS and SPOTS vs USUAL and ALARMS", () => {
  const s = JSON.stringify({wspr_noise: {expected: true, last_arrival_utc: 1, state: "ok"},
    wspr_spots: {expected: true, last_arrival_utc: 1, state: "quiet"}, ft8: {expected: false, last_arrival_utc: null, state: "-"}});
  assert.equal(L.cellText(row({sources: s}), "CLIENTS"), "wspr spots");
  assert.equal(L.cellText(row({}), "CLIENTS"), "✓");
  const v = JSON.stringify({wspr_spots: {last3h: 5, usual3h: 100}, ft8: {last3h: 40, usual3h: 100}});
  assert.equal(L.cellText(row({spots_vs_usual: v}), "SPOTS vs USUAL"), "wspr 5 / 100 low; ft8 40 / 100 ✓");
  assert.equal(L.cellText(row({spots_vs_usual: "{}"}), "SPOTS vs USUAL"), "—");
  assert.equal(L.cellText(row({n_open: "3"}), "ALARMS"), "3");
});

test("cellText survives broken JSON text", () => {
  assert.equal(L.cellText(row({sources: "nope", spots_vs_usual: "x"}), "CLIENTS"), "✓");
  assert.equal(L.cellText(row({sources: "nope", verdict: "grey"}), "ON AIR", NOW), "no data yet");
});

test("bannerText: a live ingest_stalled alarm, else null", () => {
  const a = [{station: "wd30", check: "ingest_stalled", state: "open", first_seen_utc: Date.UTC(2026, 10, 17, 9, 30) / 1000}];
  assert.equal(L.bannerText(a), "wd30 has stopped storing data since 2026-11-17 09:30 UTC; stations below may look silent");
  assert.equal(L.bannerText([]), null);
  assert.equal(L.bannerText([{station: "X", check: "off_air", state: "open", first_seen_utc: 1}]), null);
  assert.equal(L.bannerText(null), null);
});

test("hash round-trip", () => {
  assert.deepEqual(L.parseHash("#station=AC0G-B4"), {station: "AC0G-B4"});
  assert.equal(L.formatHash({station: "AC0G-B4"}), "#station=AC0G-B4");
  assert.deepEqual(L.parseHash(L.formatHash({station: "A B/C"})), {station: "A B/C"});
  assert.deepEqual(L.parseHash(""), {station: null});
  assert.equal(L.formatHash({station: null}), "#");
});

test("detailLines: numbers as key: value", () => {
  assert.deepEqual(L.detailLines(JSON.stringify({gap_s: 4000, usual: 12})), ["gap_s: 4000", "usual: 12"]);
  assert.deepEqual(L.detailLines("bad"), []);
});

test("fetchTable falls through to the next endpoint and coerces numbers", async () => {
  const seen = [];
  const f = async (url) => {
    seen.push(url);
    if (seen.length === 1) throw new Error("down");
    return {ok: true, json: async () => ({meta: [{name: "rank", type: "UInt16"}, {name: "n_open", type: "UInt64"}],
      data: [{rank: 0, n_open: "2"}]})};
  };
  const r = await L.fetchTable(f, "fleet_board", 1000);
  assert.equal(seen.length, 2);
  assert.match(decodeURIComponent(seen[0]), /SELECT \* FROM sigmondb_public\.fleet_board FORMAT JSON/);
  assert.deepEqual(r.rows, [{rank: 0, n_open: 2}]);
});

test("loadAll with no endpoint answering reports the error and the last good time", async () => {
  const f = async () => { throw new Error("down"); };
  const bad = await L.loadAll(f, null, NOW, 50);
  assert.equal(bad.data, null);
  assert.match(bad.error, /No public endpoint answered/);
  const again = await L.loadAll(f, {data: {board: [], alarms: []}, asOfMs: NOW - 120000}, NOW, 50);
  assert.equal(again.data, null, "old rows are never kept on screen");
  assert.equal(again.lastGoodMs, NOW - 120000);
});

test("stale board: ON AIR says stale, off keeps its age, no tick survives", () => {
  assert.equal(L.cellText(row({}), "ON AIR", NOW, true), "stale");
  const old = JSON.stringify({wspr_noise: {expected: true, last_arrival_utc: NOW / 1000 - 7200, state: "quiet"}});
  assert.equal(L.cellText(row({verdict: "off", sources: old}), "ON AIR", NOW, true), "✗ 2 h");
  assert.ok(!L.cellText(row({}), "CLIENTS", NOW, true).includes("✓"));
  const v = JSON.stringify({wspr_spots: {last3h: 40, usual3h: 100}, ft8: {last3h: 5, usual3h: 100}});
  const t = L.cellText(row({spots_vs_usual: v}), "SPOTS vs USUAL", NOW, true);
  assert.equal(t, "wspr 40 / 100; ft8 5 / 100 low");
  assert.ok(!t.includes("✓"));
});

test("null entry in spots_vs_usual does not throw", () => {
  const v = JSON.stringify({ft8: null, wspr_spots: {last3h: 1, usual3h: 100}});
  assert.equal(L.cellText(row({spots_vs_usual: v}), "SPOTS vs USUAL"), "wspr 1 / 100 low");
});

test("detailLines stringifies nested values", () => {
  assert.deepEqual(L.detailLines(JSON.stringify({a: {b: 1}, c: 2})), ['a: {"b":1}', "c: 2"]);
});

test("ALARMS shows acknowledged alarms beside the open count", () => {
  assert.equal(L.cellText(row({n_open: 2, n_acked: 0}), "ALARMS"), "2");
  assert.equal(L.cellText(row({n_open: 1, n_acked: 2}), "ALARMS"), "1 (+2 acked)");
  assert.equal(L.cellText(row({n_open: 0, n_acked: "1"}), "ALARMS"), "0 (+1 acked)");
  assert.equal(L.cellText(row({n_open: 0}), "ALARMS"), "0");
});

test("alarmHeading marks an acknowledged alarm", () => {
  assert.equal(L.alarmHeading({check: "off_air", subject: "", state: "open"}), "off_air (open)");
  assert.equal(L.alarmHeading({check: "client_quiet", subject: "ft8", state: "acknowledged"}),
    "client_quiet ft8 (ACKNOWLEDGED: an admin is on it)");
});
