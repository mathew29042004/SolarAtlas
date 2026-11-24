// SolarAtlas dashboard. No build step: plain JS + MapLibre.
const $ = (id) => document.getElementById(id);
const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
const fmt = (n, d = 0) => n == null ? "–" : Number(n).toLocaleString("en-IN", { maximumFractionDigits: d });
const inr = (n) => n == null ? "–" : "₹" + fmt(n);
const cr = (n) => n == null ? "–" : "₹" + fmt(n, 0) + " cr";
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const api = (p) => fetch(p).then((r) => { if (!r.ok) throw new Error(p + " " + r.status); return r.json(); });
const TYPE_LABEL = { residential: "Homes", commercial: "Shops & offices", industrial: "Factories", institutional: "Schools, hospitals, temples" };
const RAMP = ["--s1", "--s2", "--s3", "--s4", "--s5"];
const BUILDING_ZOOM = 15.5, CITY_ZOOM = 10;
let districts = [];

// ---------- tabs ----------
const loaded = {};
document.querySelectorAll(".tabs button").forEach((b) => b.addEventListener("click", () => {
  document.querySelectorAll(".tabs button").forEach((x) => x.setAttribute("aria-selected", x === b));
  document.querySelectorAll("[data-panel]").forEach((p) => p.hidden = p.dataset.panel !== b.dataset.tab);
  const t = b.dataset.tab;
  if (!loaded[t]) { loaded[t] = true; ({ rankings: loadRankings, scenarios: loadScenarios, installers: loadRoofs, ops: loadOps }[t] || (() => {}))(); }
}));

function table(el, cols, rows, onClick) {
  el.innerHTML = `<thead><tr>${cols.map((c) => `<th>${c[0]}</th>`).join("")}</tr></thead><tbody>${
    rows.map((r, i) => `<tr data-i="${i}">${cols.map((c) => `<td>${c[1](r)}</td>`).join("")}</tr>`).join("")}</tbody>`;
  if (onClick) el.querySelectorAll("tbody tr").forEach((tr) => tr.addEventListener("click", () => onClick(rows[+tr.dataset.i])));
}

// ---------- overview ----------
async function loadSummary() {
  const s = await api("/api/summary");
  $("regionName").textContent = `${s.name} · ${fmt(s.districts)} districts · Overture ${s.release}`;
  $("stats").innerHTML = [
    [fmt(s.buildings), "buildings analysed"],
    [fmt(s.potential_mwp / 1000, 1) + " GW", "rooftop potential"],
    [fmt(s.potential_gwh_year / 1000, 1) + " TWh", "per year"],
    [fmt(s.viable_buildings), "roofs that pay back ≤ 7 yrs"],
    [fmt(s.viable_mwp / 1000, 1) + " GW", "viable capacity"],
    [fmt(s.co2_kt_per_year / 1000, 1) + " Mt", "CO₂ avoided / yr"],
  ].map(([b, t]) => `<div class="stat"><b>${b}</b><small>${t}</small></div>`).join("");
  const max = Math.max(...s.by_type.map((r) => r.mwp));
  $("types").innerHTML = s.by_type.map((r) => `<div><span>${TYPE_LABEL[r.building_type] || r.building_type}</span>
    <i style="width:${(100 * r.mwp / max).toFixed(1)}%"></i><span>${fmt(r.mwp / 1000, 2)} GW</span></div>`).join("") +
    `<p class="note">Median payback: ${s.by_type.map((r) => `${TYPE_LABEL[r.building_type] || r.building_type} ${r.median_payback ?? "–"} yrs`).join(" · ")}</p>`;
}

async function loadCalc() {
  const u = +$("units").value, c0 = map.getCenter();
  $("unitsOut").textContent = u;
  const c = await api(`/api/calculator?units=${u}&lon=${c0.lng.toFixed(3)}&lat=${c0.lat.toFixed(3)}`).catch(() => api(`/api/calculator?units=${u}`));
  const r = c.recommended;
  $("calc").innerHTML = [
    ["Best size", r.kwp + " kWp"], ["Bill now", inr(r.bill_before_inr) + " / 2 mo"],
    ["Yearly saving", inr(r.annual_saving_inr)], ["Subsidy (PM Surya Ghar)", inr(r.subsidy_inr)],
    ["Your cost", inr(r.net_cost_inr)], ["Payback", r.payback_years ? r.payback_years + " yrs" : "never"],
  ].map(([k, v]) => `<div><small>${k}</small><b>${v}</b></div>`).join("");
  const v = $("verdict");
  v.className = "verdict " + (c.worth_it ? "good" : "bad");
  v.textContent = c.worth_it ? `Worth it: about ${inr(r.lifetime_saving_inr)} saved over 25 years`
                             : "Not worth it at this usage: the free units already cover most of your bill";
  loadMetering(u, r.kwp);
  $("tariffNote").innerHTML = `Tariff ${esc(c.tariff.version)} ${c.tariff.verified ? "" : '<span class="badge">unverified</span>'}
    · ${fmt(c.annual_yield_kwh_kwp)} kWh per kWp per year at the map centre`;
}

async function loadMetering(units, kwp) {
  const c0 = map.getCenter();
  let m;
  try { m = await api(`/api/metering?units=${units}&kwp=${kwp}&lon=${c0.lng.toFixed(3)}&lat=${c0.lat.toFixed(3)}`); }
  catch (e) { try { m = await api(`/api/metering?units=${units}&kwp=${kwp}`); } catch (e2) { $("meterCard").hidden = true; return; } }
  $("meterCard").hidden = false;
  const W = 340, H = 140, L = 30, B = 20, T = 10, d = m.typical_day;
  const max = Math.max(...d.map((r) => Math.max(r.pv_kwh, r.load_kwh))) * 1.1 || 1;
  const xs = (h) => L + h / 23 * (W - L - 6), ys = (v) => T + (1 - v / max) * (H - T - B);
  let g = "";
  for (const h of [0, 6, 12, 18, 23]) g += `<text x="${xs(h) - 6}" y="${H - 5}">${h}:00</text>`;
  g += `<line x1="${L}" x2="${W - 6}" y1="${ys(0)}" y2="${ys(0)}" stroke="${css("--line")}"/>`;
  g += `<text x="2" y="${ys(max / 1.1) + 3}">${fmt(max / 1.1, 1)}</text><text x="2" y="${ys(0) + 3}">0</text>`;
  const area = (k) => `M${xs(0)},${ys(0)} ` + d.map((r) => `L${xs(r.hour).toFixed(1)},${ys(r[k]).toFixed(1)}`).join(" ") + ` L${xs(23)},${ys(0)} Z`;
  g += `<path d="${area("pv_kwh")}" fill="${css("--s2")}" fill-opacity="0.55" stroke="${css("--accent")}" stroke-width="1.5"/>`;
  g += `<polyline points="${d.map((r) => `${xs(r.hour).toFixed(1)},${ys(r.load_kwh).toFixed(1)}`).join(" ")}" fill="none" stroke="${css("--ink")}" stroke-width="1.5" stroke-dasharray="4 2"/>`;
  g += `<text x="${W - 150}" y="${T + 4}">— — home use   ▬ solar (kWh/h)</text>`;
  $("dayChart").innerHTML = g;
  $("meterStats").innerHTML = [
    ["Solar used at home", fmt(m.self_consumption_ratio * 100) + "%"],
    ["Home covered by solar", fmt(m.self_sufficiency * 100) + "%"],
    ["Exported to grid", fmt(m.export_kwh) + " kWh/yr"],
    ["Still imported", fmt(m.import_kwh) + " kWh/yr"],
  ].map(([k, v]) => `<div><small>${k}</small><b>${v}</b></div>`).join("");
  $("regimeTable").innerHTML = `<thead><tr><th>Metering rule</th><th>Yearly saving</th></tr></thead><tbody>${
    Object.values(m.regimes).map((r) => `<tr><td>${esc(r.label)}</td><td>${inr(r.annual_saving_inr)}</td></tr>`).join("")}</tbody>`;
}

async function loadCurve() {
  const d = (await api("/api/economics/curve")).filter((r) => r.payback_years != null && r.payback_years <= 30);
  const W = 340, H = 150, L = 28, B = 22, T = 8;
  const xs = (u) => L + (u - 100) / 1900 * (W - L - 6), ys = (y) => T + (1 - y / 30) * (H - T - B);
  let g = "";
  for (const y of [0, 10, 20, 30]) g += `<line x1="${L}" x2="${W - 6}" y1="${ys(y)}" y2="${ys(y)}" stroke="${css("--line")}"/><text x="2" y="${ys(y) + 3}">${y}y</text>`;
  for (const u of [100, 500, 1000, 1500, 2000]) g += `<text x="${xs(u) - 10}" y="${H - 6}">${u}</text>`;
  g += `<line x1="${xs(500)}" x2="${xs(500)}" y1="${T}" y2="${H - B}" stroke="${css("--muted")}" stroke-dasharray="3 3"/>
        <text x="${xs(500) + 4}" y="${T + 10}">500-unit cliff</text>`;
  g += `<polyline points="${d.map((r) => `${xs(r.bimonthly_units).toFixed(1)},${ys(r.payback_years).toFixed(1)}`).join(" ")}" fill="none" stroke="${css("--accent")}" stroke-width="2"/>`;
  g += `<text x="${W - 118}" y="${T + 24}">units per 2-month bill →</text>`;
  $("curve").innerHTML = g;
}

// ---------- rankings ----------
async function loadRankings() {
  districts = districts.length ? districts : await api("/api/districts");
  table($("districtTable"), [
    ["District", (r) => esc(r.district)], ["Buildings", (r) => fmt(r.buildings)],
    ["Potential", (r) => fmt(r.potential_mwp) + " MW"], ["Viable", (r) => fmt(r.viable_mwp) + " MW"],
    ["Invest", (r) => cr(r.viable_investment_cr)], ["Payback", (r) => (r.median_payback_years ?? "–") + " y"],
  ], districts, (r) => { flyTo(r.lon, r.lat, 10.5); loadWards(r.district); });
  loadWards(null);
}

async function loadWards(district) {
  const rows = await api(`/api/localities?limit=25${district ? "&district=" + encodeURIComponent(district) : ""}`);
  $("wardTitle").textContent = district ? `Top wards and towns in ${district}` : "Top wards and towns in the state";
  table($("wardTable"), [
    ["Ward / town", (r) => esc(r.locality) + (district ? "" : ` <small class="note">${esc(r.district)}</small>`)],
    ["Buildings", (r) => fmt(r.buildings)], ["Potential", (r) => fmt(r.potential_mwp, 1) + " MW"],
    ["Viable roofs", (r) => fmt(r.viable_buildings)],
  ], rows, (r) => flyTo(r.lon, r.lat, 14));
}

// ---------- scenarios ----------
async function loadScenarios() {
  districts = districts.length ? districts : await api("/api/districts");
  const sel = $("scSelect");
  if (sel.options.length === 1) {
    sel.innerHTML += districts.map((d) => `<option>${esc(d.district)}</option>`).join("");
    sel.addEventListener("change", renderScenarios);
  }
  renderScenarios();
}

async function renderScenarios() {
  const d = $("scSelect").value;
  $("scDistrict").textContent = d || "Tamil Nadu";
  const rows = await api(`/api/scenarios${d ? "?district=" + encodeURIComponent(d) : ""}`);
  const delta = (v, invert) => v == null || v === 0 ? "" :
    ` <span class="${(v > 0) !== !!invert ? "up" : "down"}">${v > 0 ? "+" : ""}${fmt(v, 1)}</span>`;
  $("scenarioCards").innerHTML = rows.map((r) => `<div class="sc"><h3>${esc(r.label)}</h3><div class="row">
    <div><small>Homes viable</small><b>${fmt(r.viable_homes)}</b>${delta(r.delta_viable_homes)}</div>
    <div><small>Subsidy outlay</small><b>${cr(r.subsidy_cr)}</b>${delta(r.delta_subsidy_cr, true)}</div>
    <div><small>Viable capacity</small><b>${fmt(r.viable_mwp)} MW</b></div>
    <div><small>Businesses viable</small><b>${fmt(r.viable_nondomestic)}</b></div>
    <div><small>Investment</small><b>${cr(r.investment_cr)}</b></div>
    <div><small>Subsidy per extra home</small><b>${r.subsidy_per_extra_home_inr ? inr(r.subsidy_per_extra_home_inr) : "–"}</b></div>
  </div></div>`).join("");
}

// ---------- installers ----------
async function loadRoofs() {
  districts = districts.length ? districts : await api("/api/districts");
  const sel = $("instDistrict");
  if (sel.options.length === 1) {
    sel.innerHTML += districts.map((d) => `<option>${esc(d.district)}</option>`).join("");
    sel.addEventListener("change", loadRoofs);
    $("instType").addEventListener("change", loadRoofs);
  }
  const q = new URLSearchParams({ limit: 50, building_type: $("instType").value });
  if (sel.value) q.set("district", sel.value);
  const rows = await api(`/api/top-roofs?${q}`);
  table($("roofTable"), [
    ["Where", (r) => `${esc(r.locality || "")}<br><small class="note">${esc(r.district)}</small>`],
    ["System", (r) => fmt(r.system_kwp) + " kW"], ["kWh/yr", (r) => fmt(r.annual_kwh)],
    ["Invest", (r) => inr(r.capex_inr)], ["Payback", (r) => r.payback_years + " y"],
  ], rows, (r) => flyTo(r.lon, r.lat, 17.5));
}

// ---------- ops ----------
async function loadOps() {
  const o = await api("/api/ops");
  table($("runTable"), [
    ["Run", (r) => esc(r.run_id.slice(0, 16))], ["Region", (r) => esc(r.region)],
    ["Status", (r) => `<span class="${r.status === "success" ? "pass" : "fail"}">${esc(r.status)}</span>`],
    ["Time", (r) => fmt(r.seconds / 60, 1) + " min"],
  ], o.runs || []);
  table($("dqTable"), [
    ["Check", (r) => esc(r.check)], ["Result", (r) => `<span class="${r.passed ? "pass" : "fail"}">${r.passed ? "PASS" : "FAIL"}</span>`],
    ["Observed", (r) => esc(r.observed)], ["Rule", (r) => esc(r.expected)],
  ], o.quality || []);
  const m = o.classifier;
  $("model").innerHTML = !m ? "No model yet." : m.method === "rules" ? "Rule-based (too few labels)." : `
    <div class="result"><div><small>Accuracy (held-out)</small><b>${fmt(m.accuracy * 100, 1)}%</b></div>
    <div><small>Macro F1</small><b>${fmt(m.macro_f1, 2)}</b></div>
    <div><small>Training labels</small><b>${fmt(Object.values(m.labels).reduce((a, b) => a + b, 0))}</b></div>
    <div><small>Test rows</small><b>${fmt(m.test_rows)}</b></div></div>
    <table style="margin-top:8px"><thead><tr><th>Class</th><th>Precision</th><th>Recall</th><th>F1</th><th>Labels</th></tr></thead><tbody>
    ${Object.entries(m.per_class).map(([k, v]) => `<tr><td>${esc(k)}</td><td>${v.precision}</td><td>${v.recall}</td><td>${v.f1}</td><td>${fmt(m.labels[k])}</td></tr>`).join("")}
    </tbody></table><p class="note">Top features: ${m.top_features.slice(0, 5).map((f) => esc(f[0])).join(", ")}</p>`;
}

// ---------- search ----------
let st;
$("q").addEventListener("input", () => {
  clearTimeout(st);
  const q = $("q").value.trim();
  if (q.length < 2) { $("results").hidden = true; return; }
  st = setTimeout(async () => {
    const rs = await api(`/api/search?q=${encodeURIComponent(q)}`);
    $("results").innerHTML = rs.length ? rs.map((r, i) => `<button data-i="${i}">${esc(r.name)}
      <small>${r.kind === "district" ? "district" : esc(r.district)} · ${fmt(r.potential_mwp)} MW</small></button>`).join("")
      : `<button disabled>No match</button>`;
    $("results").hidden = false;
    $("results").querySelectorAll("button[data-i]").forEach((b) => b.addEventListener("click", () => {
      const r = rs[+b.dataset.i]; flyTo(r.lon, r.lat, r.kind === "district" ? 10.5 : 14);
      $("results").hidden = true; $("q").value = r.name;
    }));
  }, 200);
});
document.addEventListener("click", (e) => { if (!e.target.closest(".search")) $("results").hidden = true; });

// ---------- map ----------
const map = new maplibregl.Map({
  container: "map",
  style: { version: 8, sources: { osm: { type: "raster", tileSize: 256,
    tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
    attribution: "© OpenStreetMap contributors · Buildings © Overture Maps Foundation (ODbL) · Solar: NASA POWER" } },
    layers: [{ id: "osm", type: "raster", source: "osm", paint: { "raster-saturation": -0.7, "raster-opacity": 0.85 } }] },
  center: [78.6, 11.0], zoom: 6.4, maxZoom: 19,
});
map.addControl(new maplibregl.NavigationControl(), "top-right");
const flyTo = (lon, lat, zoom) => map.flyTo({ center: [lon, lat], zoom, speed: 1.6 });

function quantileRamp(values, expr) {
  const v = values.filter((x) => x != null).sort((a, b) => a - b);
  const q = (p) => v[Math.floor(p * (v.length - 1))] || 0;
  const stops = [q(0.05), q(0.3), q(0.6), q(0.85), q(0.99)];
  for (let i = 1; i < stops.length; i++) if (stops[i] <= stops[i - 1]) stops[i] = stops[i - 1] + 1e-9;
  return ["interpolate", ["linear"], expr, ...stops.flatMap((s, i) => [s, css(RAMP[i])])];
}

map.on("load", async () => {
  const r7 = await api("/api/hexes?res=7");
  map.addSource("hex7", { type: "geojson", data: r7 });
  map.addLayer({ id: "hex7", type: "fill", source: "hex7", maxzoom: CITY_ZOOM,
    paint: { "fill-color": quantileRamp(r7.features.map((f) => f.properties.potential_gwh_year), ["get", "potential_gwh_year"]),
             "fill-opacity": 0.72, "fill-outline-color": "rgba(0,0,0,0.12)" } });
  map.addSource("hex8", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addLayer({ id: "hex8", type: "fill", source: "hex8", minzoom: CITY_ZOOM, maxzoom: BUILDING_ZOOM,
    paint: { "fill-color": "#ccc", "fill-opacity": 0.72, "fill-outline-color": "rgba(0,0,0,0.15)" } });
  map.addSource("bld", { type: "geojson", data: { type: "FeatureCollection", features: [] } });
  map.addLayer({ id: "bld", type: "fill", source: "bld", minzoom: BUILDING_ZOOM,
    paint: { "fill-color": ["interpolate", ["linear"], ["get", "annual_kwh"], 0, css("--s1"), 2000, css("--s2"),
             5000, css("--s3"), 15000, css("--s4"), 60000, css("--s5")], "fill-opacity": 0.85, "fill-outline-color": "rgba(0,0,0,0.35)" } });
  map.addLayer({ id: "bld-viable", type: "line", source: "bld", minzoom: BUILDING_ZOOM,
    filter: ["==", ["get", "viable"], true], paint: { "line-color": css("--good"), "line-width": 1.2 } });

  for (const l of ["hex7", "hex8"]) {
    map.on("click", l, (e) => {
      const p = e.features[0].properties;
      new maplibregl.Popup().setLngLat(e.lngLat).setHTML(`<div class="pop"><b>${fmt(p.buildings)} buildings</b>
        ${p.district ? `<br><small>${esc(p.district)}</small>` : ""}<table>
        <tr><td>Potential</td><td>${fmt(p.potential_mwp, 1)} MW</td></tr>
        <tr><td>Energy</td><td>${fmt(p.potential_gwh_year * 1000)} MWh/yr</td></tr>
        <tr><td>Viable roofs</td><td>${fmt(p.viable_buildings)}</td></tr></table><small>Zoom in for more detail</small></div>`).addTo(map);
    });
  }
  map.on("click", "bld", async (e) => {
    const id = e.features[0].properties.id;
    const b = await api(`/api/buildings/${encodeURIComponent(id)}`);
    const h = b.household?.recommended, biz = b.business;
    const econ = h ? `<tr><td colspan="2"><small>Home using ~${b.household.units_bimonthly} units/bill:</small></td></tr>
        <tr><td>${h.kwp} kWp, your cost</td><td>${inr(h.net_cost_inr)}</td></tr>
        <tr><td>Payback</td><td>${h.payback_years ? h.payback_years + " yrs" : "never"}</td></tr>`
      : biz ? `<tr><td>${biz.kwp} kWp, invest</td><td>${inr(biz.capex_inr)}</td></tr>
        <tr><td>Payback</td><td>${biz.payback_years ? biz.payback_years + " yrs" : "never"}</td></tr>`
      : `<tr><td colspan="2"><small>Roof too small for a system</small></td></tr>`;
    new maplibregl.Popup().setLngLat(e.lngLat).setHTML(`<div class="pop"><b>${fmt(b.footprint_m2)} m² · ${esc(TYPE_LABEL[b.building_type] || b.building_type)}</b>
      <small>${b.type_source === "osm" ? "(tagged)" : b.building_type === "residential" ? "(model)" : `(model, ${fmt((b.type_prob || 0) * 100)}%)`}</small><table>
      <tr><td>Max system</td><td>${fmt(b.roof_kwp, 1)} kWp</td></tr>
      <tr><td>Generation</td><td>${fmt(b.annual_kwh)} kWh/yr</td></tr>
      <tr><td>Shade estimate</td><td>${fmt((b.shading_loss || 0) * 100)}%</td></tr>${econ}</table>
      <a href="/report/${encodeURIComponent(id)}" target="_blank" rel="noopener">Open full report →</a></div>`).addTo(map);
  });
  for (const l of ["hex7", "hex8", "bld"]) {
    map.on("mouseenter", l, () => map.getCanvas().style.cursor = "pointer");
    map.on("mouseleave", l, () => map.getCanvas().style.cursor = "");
  }
  map.on("moveend", refresh);
  refresh();
});

let pending, calcTimer;
async function refresh() {
  const z = map.getZoom();
  $("legendTitle").textContent = z >= BUILDING_ZOOM ? "Generation per building (kWh/yr) · green edge = pays back ≤ 7 yrs"
    : z >= CITY_ZOOM ? "Potential per hexagon (~0.7 km²)" : "Potential per hexagon (~5 km²)";
  clearTimeout(calcTimer); calcTimer = setTimeout(loadCalc, 300);
  if (z < CITY_ZOOM || !map.getSource("bld")) return;
  const b = map.getBounds();
  const pad = 0.02, bbox = [b.getWest() - pad, b.getSouth() - pad, b.getEast() + pad, b.getNorth() + pad].map((v) => v.toFixed(4)).join(",");
  const ctrl = new AbortController(); pending?.abort(); pending = ctrl;
  try {
    if (z < BUILDING_ZOOM) {
      const fc = await (await fetch(`/api/hexes?res=8&bbox=${bbox}`, { signal: ctrl.signal })).json();
      map.getSource("hex8").setData(fc);
      if (fc.features.length) map.setPaintProperty("hex8", "fill-color",
        quantileRamp(fc.features.map((f) => f.properties.potential_gwh_year), ["get", "potential_gwh_year"]));
    } else {
      const r = await fetch(`/api/buildings?bbox=${bbox.split(",").map((v, i) => (+v + (i < 2 ? pad : -pad)).toFixed(5)).join(",")}`, { signal: ctrl.signal });
      if (r.ok) map.getSource("bld").setData(await r.json());
    }
  } catch (e) { if (e.name !== "AbortError") console.error(e); }
}

$("units").addEventListener("input", () => { clearTimeout(calcTimer); calcTimer = setTimeout(loadCalc, 120); });
loadSummary(); loadCurve();
