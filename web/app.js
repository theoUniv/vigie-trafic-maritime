/* Vigie — interface de suivi du trafic maritime temps réel */
(() => {
  "use strict";

  // ------------------------------------------------------------------ utilitaires
  const $ = (sel) => document.querySelector(sel);
  const el = (tag, attrs = {}, ...children) => {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") node.className = v;
      else if (k === "style") node.style.cssText = v;
      else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
      else if (v !== null && v !== undefined) node.setAttribute(k, v);
    }
    for (const c of children.flat()) if (c !== null && c !== undefined) node.append(c);
    return node;
  };
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const store = {
    get(k, d) { try { const v = localStorage.getItem(k); return v === null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch { /* stockage indisponible */ } },
  };
  const fmt = new Intl.NumberFormat("fr-FR");
  const rtf = new Intl.RelativeTimeFormat("fr", { numeric: "auto" });
  const toMs = (t) => (typeof t === "number" ? t * 1000 : Date.parse(t));
  function ago(t) {
    const s = Math.round((toMs(t) - Date.now()) / 1000);
    const a = Math.abs(s);
    if (a < 60) return rtf.format(s, "second");
    if (a < 3600) return rtf.format(Math.round(s / 60), "minute");
    if (a < 86400) return rtf.format(Math.round(s / 3600), "hour");
    return rtf.format(Math.round(s / 86400), "day");
  }
  const dateFmt = new Intl.DateTimeFormat("fr-FR", { day: "2-digit", month: "short", hour: "2-digit", minute: "2-digit", timeZone: "UTC" });
  const fmtDate = (t) => (t ? dateFmt.format(new Date(toMs(t))) + " UTC" : "—");
  const flag = (iso) => (!iso || iso === "XX" ? "🏳️" : String.fromCodePoint(...[...iso.toUpperCase()].map((c) => 0x1f1a5 + c.charCodeAt(0))));
  const kn = (v) => (v === null || v === undefined ? "—" : `${Number(v).toFixed(1)} nd`);
  async function api(path) {
    const r = await fetch(path, { headers: { Accept: "application/json" } });
    if (!r.ok) throw new Error(`${r.status} ${path}`);
    return r.json();
  }

  const CATS = {
    cargo: "Cargo", tanker: "Pétrolier", passenger: "Passagers", fishing: "Pêche", service: "Service",
    highspeed: "Grande vitesse", pleasure: "Plaisance", authority: "Autorité", other: "Autre",
  };
  const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  let catColors = {};
  const refreshColors = () => { catColors = Object.fromEntries(Object.keys(CATS).map((c) => [c, cssVar(`--c-${c}`)])); };
  const LEVEL = {
    congestion: { label: "Congestion", color: () => cssVar("--crit") },
    ralentissement: { label: "Ralentissement", color: () => cssVar("--warn") },
    dense: { label: "Trafic dense", color: () => cssVar("--info") },
  };
  const SEV_COLOR = () => ({ critical: cssVar("--crit"), warning: cssVar("--warn"), info: cssVar("--info") });

  // ----------------------------------------------------------------------- état
  const state = {
    filters: store.get("vigie.filters", { categories: [], country: "", status: "", commercial: false }),
    vessels: [],
    selected: null,
    follow: false,
    trackHours: 6,
    alertKinds: [],
    alerts: [],
    lastOk: 0,
  };

  // ---------------------------------------------------------------------- thème
  const themeBtn = $("#theme-toggle");
  const savedTheme = store.get("vigie.theme", null);
  if (savedTheme) document.documentElement.dataset.theme = savedTheme;
  const isDark = () => {
    const t = document.documentElement.dataset.theme;
    return t ? t === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  };

  // ----------------------------------------------------------------------- carte
  const map = L.map("map", { zoomControl: true, preferCanvas: true, worldCopyJump: true }).setView([59.9, 23.5], 6);
  // Fonds de carte gris neutres (Esri Canvas, sans clé) : les navires colorés restent lisibles
  const esri = (name) => `https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/${name}/MapServer/tile/{z}/{y}/{x}`;
  const tiles = { light: esri("World_Light_Gray_Base"), dark: esri("World_Dark_Gray_Base") };
  const base = L.tileLayer(isDark() ? tiles.dark : tiles.light, {
    maxZoom: 16,
    attribution: 'Fond © <a href="https://www.esri.com">Esri</a>, HERE, Garmin, © OpenStreetMap',
  }).addTo(map);
  const seamarks = L.tileLayer("https://tiles.openseamap.org/seamark/{z}/{x}/{y}.png", {
    maxZoom: 18, attribution: '© <a href="https://www.openseamap.org">OpenSeaMap</a>',
  });
  const applyTheme = () => {
    refreshColors();
    base.setUrl(isDark() ? tiles.dark : tiles.light);
    vesselLayer.redraw();
    renderZonesLayer();
    renderLegend();
  };
  themeBtn.addEventListener("click", () => {
    const next = isDark() ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    store.set("vigie.theme", next);
    applyTheme();
  });
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => { if (!store.get("vigie.theme", null)) applyTheme(); });

  // Couche navires sur canvas (des milliers de marqueurs orientés selon le cap)
  const VesselLayer = L.Layer.extend({
    initialize() { this._data = []; this._pts = []; },
    onAdd(m) {
      this._map = m;
      this._canvas = L.DomUtil.create("canvas", "vessel-canvas leaflet-zoom-hide");
      m.getPanes().overlayPane.appendChild(this._canvas);
      m.on("moveend zoomend resize viewreset", this._reset, this);
      this._reset();
    },
    onRemove(m) {
      L.DomUtil.remove(this._canvas);
      m.off("moveend zoomend resize viewreset", this._reset, this);
    },
    setData(d) { this._data = d; this.redraw(); },
    redraw() { if (this._map) this._draw(); },
    _reset() {
      const size = this._map.getSize();
      const dpr = window.devicePixelRatio || 1;
      L.DomUtil.setPosition(this._canvas, this._map.containerPointToLayerPoint([0, 0]));
      this._canvas.width = size.x * dpr; this._canvas.height = size.y * dpr;
      this._canvas.style.width = `${size.x}px`; this._canvas.style.height = `${size.y}px`;
      this._draw();
    },
    _draw() {
      const ctx = this._canvas.getContext("2d");
      const dpr = window.devicePixelRatio || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      const size = this._map.getSize();
      ctx.clearRect(0, 0, size.x, size.y);
      const z = this._map.getZoom();
      const s = z >= 10 ? 1.35 : z >= 8 ? 1.1 : z >= 6 ? 0.9 : 0.7;
      const stroke = isDark() ? "rgba(7,17,30,.85)" : "rgba(255,255,255,.95)";
      this._pts = [];
      let selPt = null;
      for (const v of this._data) {
        const p = this._map.latLngToContainerPoint([v.lat, v.lon]);
        if (p.x < -20 || p.y < -20 || p.x > size.x + 20 || p.y > size.y + 20) continue;
        this._pts.push([p.x, p.y, v]);
        const color = catColors[v.category] || catColors.other;
        ctx.fillStyle = color;
        ctx.strokeStyle = stroke;
        ctx.lineWidth = 1;
        const moving = v.sog !== null && v.sog >= 1 && v.cog !== null;
        if (moving) {
          const a = ((v.heading ?? v.cog) * Math.PI) / 180;
          ctx.save();
          ctx.translate(p.x, p.y);
          ctx.rotate(a);
          ctx.beginPath();
          ctx.moveTo(0, -8 * s); ctx.lineTo(4.6 * s, 6 * s); ctx.lineTo(0, 3.6 * s); ctx.lineTo(-4.6 * s, 6 * s);
          ctx.closePath(); ctx.fill(); ctx.stroke();
          ctx.restore();
        } else {
          ctx.beginPath(); ctx.arc(p.x, p.y, 3.4 * s, 0, Math.PI * 2); ctx.fill(); ctx.stroke();
        }
        if (v.eta_status === "retard_probable" || v.eta_status === "retard_important") {
          ctx.beginPath(); ctx.arc(p.x + 6 * s, p.y - 6 * s, 2.4 * s, 0, Math.PI * 2);
          ctx.fillStyle = cssVar("--warn"); ctx.fill();
        }
        if (state.selected && v.mmsi === state.selected) selPt = p;
      }
      if (selPt) {
        ctx.strokeStyle = cssVar("--accent"); ctx.lineWidth = 2.5;
        ctx.beginPath(); ctx.arc(selPt.x, selPt.y, 13, 0, Math.PI * 2); ctx.stroke();
        ctx.lineWidth = 1; ctx.globalAlpha = 0.35;
        ctx.beginPath(); ctx.arc(selPt.x, selPt.y, 19, 0, Math.PI * 2); ctx.stroke();
        ctx.globalAlpha = 1;
      }
    },
    hit(cp, radius = 10) {
      let best = null, bd = radius * radius;
      for (const [x, y, v] of this._pts) {
        const d = (x - cp.x) ** 2 + (y - cp.y) ** 2;
        if (d < bd) { bd = d; best = v; }
      }
      return best;
    },
  });
  refreshColors();
  const vesselLayer = new VesselLayer().addTo(map);
  const trackLayer = L.layerGroup().addTo(map);
  const zonesLayer = L.layerGroup().addTo(map);
  const alertsLayer = L.layerGroup().addTo(map);
  L.control.layers(null, {
    "Navires": vesselLayer, "Trajectoire": trackLayer, "Zones de congestion": zonesLayer,
    "Alertes récentes": alertsLayer, "Balisage OpenSeaMap": seamarks,
  }, { position: "topright" }).addTo(map);
  L.control.scale({ imperial: false, position: "bottomright" }).addTo(map);

  const tip = $("#hover-tip");
  map.on("mousemove", (e) => {
    if (!map.hasLayer(vesselLayer)) return;
    const v = vesselLayer.hit(e.containerPoint);
    map.getContainer().style.cursor = v ? "pointer" : "";
    if (!v) { tip.hidden = true; return; }
    tip.innerHTML = `<b>${flag(v.country_code)} ${esc(v.name || `MMSI ${v.mmsi}`)}</b><span>${CATS[v.category] || "Autre"} · ${kn(v.sog)}</span>`;
    tip.style.left = `${e.containerPoint.x}px`; tip.style.top = `${e.containerPoint.y}px`;
    tip.hidden = false;
  });
  map.on("mouseout", () => { tip.hidden = true; });
  map.on("click", (e) => {
    const v = vesselLayer.hit(e.containerPoint, 12);
    if (v) selectVessel(v.mmsi, false);
  });
  map.on("dragstart", () => { if (state.follow) setFollow(false); });

  function renderLegend() {
    const legend = $("#legend");
    legend.innerHTML = "";
    for (const c of ["cargo", "tanker", "passenger", "fishing", "service", "pleasure"]) {
      legend.append(el("span", { class: "lg" }, el("span", { class: "dot", style: `background:${catColors[c]}` }), CATS[c]));
    }
    legend.append(el("span", { class: "lg" }, el("span", { class: "sq", style: `background:${LEVEL.congestion.color()}` }), "Congestion"));
    legend.append(el("span", { class: "lg" }, el("span", { class: "sq", style: `background:${LEVEL.ralentissement.color()}` }), "Ralentissement"));
  }

  // -------------------------------------------------------------------- filtres
  function queryString() {
    const p = new URLSearchParams();
    const f = state.filters;
    f.categories.forEach((c) => p.append("category", c));
    if (f.country) p.append("country", f.country);
    if (f.status) p.set("status", f.status);
    if (f.commercial) p.set("commercial", "true");
    return p.toString();
  }
  function saveFilters() { store.set("vigie.filters", state.filters); refreshVessels(); }

  function renderCategoryChips(categories) {
    const box = $("#category-chips");
    box.innerHTML = "";
    const counts = Object.fromEntries(categories.map((c) => [c.code, c.n]));
    for (const code of Object.keys(CATS)) {
      if (!counts[code] && !state.filters.categories.includes(code)) continue;
      const active = state.filters.categories.includes(code);
      box.append(el("button", {
        class: `chip${active ? " active" : ""}`, "aria-pressed": active,
        onclick: () => {
          const set = new Set(state.filters.categories);
          set.has(code) ? set.delete(code) : set.add(code);
          state.filters.categories = [...set];
          renderCategoryChips(categories); saveFilters();
        },
      }, el("span", { class: "dot", style: `background:${catColors[code]}` }), CATS[code],
      el("span", { class: "count" }, fmt.format(counts[code] || 0))));
    }
  }

  function renderCountries(countries) {
    const sel = $("#country");
    const current = state.filters.country;
    sel.innerHTML = "";
    sel.append(el("option", { value: "" }, "Tous les pays"));
    for (const c of countries) {
      const opt = el("option", { value: c.code }, `${flag(c.code)}  ${c.name} (${c.n})`);
      if (c.code === current) opt.selected = true;
      sel.append(opt);
    }
  }
  $("#country").addEventListener("change", (e) => { state.filters.country = e.target.value; saveFilters(); });

  const segButtons = [...document.querySelectorAll("#status-filter button")];
  const syncSegments = () => segButtons.forEach((b) => {
    const on = b.dataset.status === state.filters.status;
    b.classList.toggle("active", on); b.setAttribute("aria-checked", on);
  });
  segButtons.forEach((b) => b.addEventListener("click", () => { state.filters.status = b.dataset.status; syncSegments(); saveFilters(); }));
  const commercial = $("#commercial");
  commercial.addEventListener("change", () => { state.filters.commercial = commercial.checked; saveFilters(); });
  $("#reset-filters").addEventListener("click", () => {
    state.filters = { categories: [], country: "", status: "", commercial: false };
    commercial.checked = false; $("#country").value = ""; syncSegments(); saveFilters(); refreshMeta();
  });

  // -------------------------------------------------------------------- recherche
  const searchInput = $("#search");
  const results = $("#search-results");
  let searchTimer, searchIdx = -1;
  searchInput.addEventListener("input", () => {
    clearTimeout(searchTimer);
    const qv = searchInput.value.trim();
    if (qv.length < 2) { results.hidden = true; return; }
    searchTimer = setTimeout(async () => {
      try {
        const rows = await api(`/api/search?q=${encodeURIComponent(qv)}`);
        results.innerHTML = "";
        searchIdx = -1;
        if (!rows.length) results.append(el("li", { class: "muted" }, "Aucun navire trouvé"));
        for (const r of rows) {
          results.append(el("li", { "data-mmsi": r.mmsi, onclick: () => pick(r.mmsi) },
            el("span", {}, flag(r.country_code)),
            el("div", {}, el("div", { class: "sr-name" }, r.name || `MMSI ${r.mmsi}`),
              el("div", { class: "sr-meta" }, `${CATS[r.category] || "Autre"} · MMSI ${r.mmsi}${r.destination ? ` · → ${r.destination}` : ""}`))));
        }
        results.hidden = false;
      } catch { results.hidden = true; }
    }, 220);
  });
  searchInput.addEventListener("keydown", (e) => {
    const items = [...results.querySelectorAll("li[data-mmsi]")];
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      searchIdx = (searchIdx + (e.key === "ArrowDown" ? 1 : -1) + items.length) % items.length;
      items.forEach((li, i) => li.classList.toggle("active", i === searchIdx));
    } else if (e.key === "Enter" && items.length) {
      pick(Number(items[Math.max(searchIdx, 0)].dataset.mmsi));
    } else if (e.key === "Escape") { results.hidden = true; }
  });
  document.addEventListener("click", (e) => { if (!e.target.closest(".search")) results.hidden = true; });
  function pick(mmsi) { results.hidden = true; searchInput.blur(); selectVessel(mmsi, true); }

  // ---------------------------------------------------------------- fiche navire
  const drawer = $("#drawer");
  $("#d-close").addEventListener("click", closeVessel);
  document.addEventListener("keydown", (e) => { if (e.key === "Escape" && !drawer.hidden) closeVessel(); });
  $("#d-follow").addEventListener("click", () => setFollow(!state.follow));
  $("#d-hours").addEventListener("change", (e) => { state.trackHours = Number(e.target.value); loadTrack(); });

  function setFollow(on) {
    state.follow = on;
    const btn = $("#d-follow");
    btn.classList.toggle("on", on);
    btn.querySelector("span").textContent = on ? "Suivi actif" : "Suivre ce navire";
    if (on) centerOnSelected();
  }
  function centerOnSelected() {
    const v = state.vessels.find((x) => x.mmsi === state.selected) || state.detail;
    if (v && v.lat !== null && v.lat !== undefined) map.panTo([v.lat, v.lon], { animate: true });
  }
  function closeVessel() {
    state.selected = null; state.detail = null; setFollow(false);
    drawer.hidden = true; $("#app").classList.remove("drawer-open"); trackLayer.clearLayers(); vesselLayer.redraw();
  }

  async function selectVessel(mmsi, fly) {
    state.selected = mmsi;
    vesselLayer.redraw();
    drawer.hidden = false;
    $("#app").classList.add("drawer-open");
    await loadDetail();
    if (fly && state.detail && state.detail.lat !== null) map.flyTo([state.detail.lat, state.detail.lon], Math.max(map.getZoom(), 10), { duration: 0.8 });
    loadTrack();
  }

  async function loadDetail() {
    if (!state.selected) return;
    try {
      const d = await api(`/api/vessels/${state.selected}`);
      if (d.mmsi !== state.selected) return;
      state.detail = d;
      renderDetail(d);
    } catch {
      $("#d-body").innerHTML = '<p class="muted">Fiche indisponible.</p>';
    }
  }

  function renderDetail(d) {
    const color = catColors[d.category] || catColors.other;
    $("#d-name").textContent = d.name || `MMSI ${d.mmsi}`;
    $("#d-kicker").innerHTML = `<span>${flag(d.country_code)} ${esc(d.country || "Pavillon inconnu")}</span>
      <span class="pill"><span class="dot" style="background:${color}"></span>${esc(d.type_label)}</span>`;
    const est = d.eta_estimate;
    let voyage = '<p class="muted">Aucune destination exploitable déclarée.</p>';
    if (d.destination || est) {
      const badgeCls = !est ? "delay-neutral" : { a_l_heure: "delay-ok", arrive: "delay-ok", avance: "delay-ok",
        retard_probable: "delay-warn", eta_depassee: "delay-warn", retard_important: "delay-crit" }[est.status] || "delay-neutral";
      const delay = est && est.delay_min !== null ? ` (${est.delay_min > 0 ? "+" : ""}${fmt.format(est.delay_min)} min)` : "";
      voyage = `<div class="voyage">
        <div class="voyage-route"><span>${esc(d.destination || "—")}</span>${est ? `<span class="arrow">→</span><span>${esc(est.dest_name)} <span class="muted mono">${esc(est.dest_locode)}</span></span>` : ""}</div>
        ${est ? `<span class="delay-badge ${badgeCls}">${esc(est.status_label)}${delay}</span>` : '<span class="delay-badge delay-neutral">Port de destination non reconnu</span>'}
        <div class="d-grid">
          <div class="d-item"><span>ETA déclarée</span><b>${fmtDate(est?.eta_declared || d.eta)}</b></div>
          <div class="d-item"><span>ETA estimée</span><b>${fmtDate(est?.eta_predicted)}</b></div>
          <div class="d-item"><span>Distance restante</span><b class="mono">${est ? `${fmt.format(est.remaining_nm)} nm` : "—"}</b></div>
          <div class="d-item"><span>Base de calcul</span><b>${est?.basis ? esc(est.basis) : "—"}</b></div>
        </div></div>`;
    }
    const anoms = d.anomalies.length
      ? `<ul class="d-anoms">${d.anomalies.map((a) => `<li class="sev-${a.severity}"><b>${esc(a.label)}</b> · <span class="muted">${ago(a.ts)}</span><br>${esc(a.message)}</li>`).join("")}</ul>`
      : '<p class="muted">Aucune anomalie détectée.</p>';
    $("#d-body").innerHTML = `
      <div class="d-section d-big">
        <div><b>${d.sog !== null ? Number(d.sog).toFixed(1) : "—"}</b><span>Vitesse (nd)</span></div>
        <div><b>${d.cog !== null ? `${Math.round(d.cog)}°` : "—"}</b><span>Cap fond</span></div>
        <div><b>${d.avg_sog_30m !== null ? Number(d.avg_sog_30m).toFixed(1) : "—"}</b><span>Moy. 30 min</span></div>
      </div>
      <div class="d-section d-grid">
        <div class="d-item"><span>Statut</span><b>${esc(d.nav_status_label)}</b></div>
        <div class="d-item"><span>Dernier signal</span><b>${d.position_ts ? ago(d.position_ts) : "—"}</b></div>
        <div class="d-item"><span>MMSI</span><b class="mono">${d.mmsi}</b></div>
        <div class="d-item"><span>IMO</span><b class="mono">${d.imo || "—"}</b></div>
        <div class="d-item"><span>Indicatif</span><b class="mono">${esc(d.callsign || "—")}</b></div>
        <div class="d-item"><span>Dimensions</span><b>${d.length ? `${d.length} × ${d.width || "?"} m` : "—"}</b></div>
        <div class="d-item"><span>Tirant d'eau</span><b>${d.draught ? `${d.draught} m` : "—"}</b></div>
        <div class="d-item"><span>Position</span><b class="mono">${d.lat !== null ? `${d.lat.toFixed(3)}, ${d.lon.toFixed(3)}` : "—"}</b></div>
      </div>
      <div class="d-section"><h3>Voyage</h3>${voyage}</div>
      <div class="d-section"><h3>Profil de vitesse</h3><div class="speed-chart" id="d-speed"><p class="muted">Chargement…</p></div></div>
      <div class="d-section"><h3>Anomalies</h3>${anoms}</div>`;
    if (state.trackPoints) renderSpeedChart(state.trackPoints);
  }

  async function loadTrack() {
    if (!state.selected) return;
    const mmsi = state.selected;
    try {
      const t = await api(`/api/vessels/${mmsi}/track?hours=${state.trackHours}`);
      if (mmsi !== state.selected) return;
      state.trackPoints = t.points;
      trackLayer.clearLayers();
      const pts = t.points;
      if (pts.length > 1) {
        const accent = cssVar("--accent");
        L.polyline(pts.map((p) => [p.lat, p.lon]), { color: accent, weight: 6, opacity: 0.18, interactive: false }).addTo(trackLayer);
        L.polyline(pts.map((p) => [p.lat, p.lon]), { color: accent, weight: 2.2, opacity: 0.95, dashArray: null, interactive: false }).addTo(trackLayer);
        L.circleMarker([pts[0].lat, pts[0].lon], { radius: 4, color: accent, fillColor: cssVar("--panel"), fillOpacity: 1, weight: 2 })
          .bindTooltip(`Début de trajectoire · ${fmtDate(pts[0].ts)}`).addTo(trackLayer);
      }
      const est = state.detail?.eta_estimate;
      if (est && est.dest_lat !== undefined && pts.length) {
        const last = pts[pts.length - 1];
        L.polyline([[last.lat, last.lon], [est.dest_lat, est.dest_lon]], { color: cssVar("--muted"), weight: 1.5, dashArray: "4 6", interactive: false }).addTo(trackLayer);
        L.circleMarker([est.dest_lat, est.dest_lon], { radius: 5, color: cssVar("--muted"), weight: 2, fillOpacity: 0 })
          .bindTooltip(`Destination : ${esc(est.dest_name)}`).addTo(trackLayer);
      }
      renderSpeedChart(pts);
    } catch { /* trajectoire indisponible */ }
  }

  function renderSpeedChart(pts) {
    const box = document.getElementById("d-speed");
    if (!box) return;
    const data = pts.filter((p) => p.sog !== null);
    if (data.length < 2) { box.innerHTML = '<p class="muted">Pas encore assez de positions reçues.</p>'; return; }
    const W = Math.max(200, box.clientWidth || 320), H = 90, pad = { l: 26, r: 4, t: 6, b: 16 };
    const t0 = data[0].ts, t1 = data[data.length - 1].ts;
    const vmax = Math.max(5, ...data.map((p) => p.sog)) * 1.1;
    const x = (t) => pad.l + ((t - t0) / Math.max(1, t1 - t0)) * (W - pad.l - pad.r);
    const y = (v) => H - pad.b - (v / vmax) * (H - pad.t - pad.b);
    const line = data.map((p, i) => `${i ? "L" : "M"}${x(p.ts).toFixed(1)},${y(p.sog).toFixed(1)}`).join("");
    const area = `${line}L${x(t1).toFixed(1)},${y(0)}L${x(t0).toFixed(1)},${y(0)}Z`;
    const accent = cssVar("--accent");
    const hhmm = (t) => new Date(t * 1000).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" });
    box.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Vitesse du navire dans le temps">
      <line x1="${pad.l}" x2="${W - pad.r}" y1="${y(0)}" y2="${y(0)}" stroke="${cssVar("--line")}" />
      <line x1="${pad.l}" x2="${W - pad.r}" y1="${y(vmax / 1.1 / 2)}" y2="${y(vmax / 1.1 / 2)}" stroke="${cssVar("--line")}" stroke-dasharray="3 4" />
      <path d="${area}" fill="${accent}" opacity=".14" stroke="none"/>
      <path d="${line}" fill="none" stroke="${accent}" stroke-width="1.8" />
      <text class="axis" x="2" y="${y(vmax / 1.1) + 4}">${Math.round(vmax / 1.1)}</text>
      <text class="axis" x="2" y="${y(0)}">0</text>
      <text class="axis" x="${pad.l}" y="${H - 2}">${hhmm(t0)}</text>
      <text class="axis" x="${W - pad.r}" y="${H - 2}" text-anchor="end">${hhmm(t1)}</text>
    </svg>`;
  }

  // --------------------------------------------------------------- alertes / zones
  function renderAlertKinds(kinds) {
    const box = $("#alert-kinds");
    box.innerHTML = "";
    for (const [code, label] of Object.entries(kinds)) {
      const active = state.alertKinds.includes(code);
      box.append(el("button", {
        class: `chip${active ? " active" : ""}`, "aria-pressed": active,
        onclick: () => {
          const s = new Set(state.alertKinds);
          s.has(code) ? s.delete(code) : s.add(code);
          state.alertKinds = [...s];
          renderAlertKinds(kinds); refreshAlerts();
        },
      }, label));
    }
  }

  async function refreshAlerts() {
    const p = new URLSearchParams({ hours: "6", limit: "200" });
    state.alertKinds.forEach((k) => p.append("kind", k));
    try {
      const rows = await api(`/api/anomalies?${p}`);
      state.alerts = rows;
      const feed = $("#alert-feed");
      feed.innerHTML = "";
      if (!rows.length) feed.append(el("li", { class: "empty" }, "Aucune alerte sur les 6 dernières heures."));
      for (const a of rows.slice(0, 120)) {
        feed.append(el("li", {
          class: `sev-${a.severity}`,
          onclick: () => (a.mmsi ? selectVessel(a.mmsi, true) : map.flyTo([a.lat, a.lon], 11, { duration: 0.8 })),
        },
        el("div", { class: "f-top" }, el("span", { class: "f-kind" }, a.label), el("span", {}, ago(a.ts))),
        a.mmsi ? el("div", { class: "f-vessel" }, `${flag(a.country_code)} ${a.vessel_name || `MMSI ${a.mmsi}`}`) : null,
        el("div", { class: "f-msg" }, a.message)));
      }
      $("#badge-alerts").textContent = rows.length ? (rows.length > 99 ? "99+" : String(rows.length)) : "";
      // Marqueurs : alertes de la dernière heure
      alertsLayer.clearLayers();
      const colors = SEV_COLOR();
      // Congestion (couche Zones) et retards (pastille sur le navire) ont déjà leur représentation
      const recent = rows.filter((a) => Date.now() - toMs(a.ts) < 3600e3 && !["CONGESTION", "DELAY"].includes(a.kind));
      for (const a of recent.slice(0, 150)) {
        const icon = L.divIcon({ className: "", html: `<div class="alert-pin" style="background:${colors[a.severity]};color:${colors[a.severity]}"></div>`, iconSize: [14, 14] });
        L.marker([a.lat, a.lon], { icon, keyboard: false, zIndexOffset: -100 })
          .bindTooltip(`<b>${esc(a.label)}</b><br>${esc(a.vessel_name || "")} ${esc(a.message)}`, { direction: "top", offset: [0, -8] })
          .on("click", () => a.mmsi && selectVessel(a.mmsi, false))
          .addTo(alertsLayer);
      }
    } catch { /* API indisponible : on réessaie au prochain cycle */ }
  }

  let zones = [];
  const zoneTip = (z, lv) => `<b>${lv.label}${z.place ? ` · ${esc(z.place)}` : ""}</b><br>${z.n_vessels} navires · ${z.n_waiting} cargos en attente<br>Vitesse moy. ${Number(z.mean_sog).toFixed(1)} nd${z.baseline_sog ? ` (normale ${Number(z.baseline_sog).toFixed(1)} nd)` : ""}`;
  function renderZonesLayer() {
    zonesLayer.clearLayers();
    const detailed = map.getZoom() >= 9;
    for (const z of zones) {
      const lv = LEVEL[z.level];
      if (!lv) continue;
      const c = lv.color();
      if (detailed) {
        L.rectangle([[z.lat_min, z.lon_min], [z.lat_max, z.lon_max]], {
          color: c, weight: z.level === "dense" ? 1 : 1.6, fillColor: c, fillOpacity: z.level === "dense" ? 0.08 : 0.22,
        }).bindTooltip(zoneTip(z, lv), { sticky: true }).addTo(zonesLayer);
      } else if (z.level !== "dense") {
        // Vue régionale : une cellule de 5 km serait invisible, on dessine un halo proportionnel
        L.circleMarker([(z.lat_min + z.lat_max) / 2, (z.lon_min + z.lon_max) / 2], {
          radius: 7 + Math.min(14, z.n_vessels * 1.4), color: c, weight: 1.5, fillColor: c, fillOpacity: 0.18,
        }).bindTooltip(zoneTip(z, lv), { sticky: true })
          .on("click", () => map.flyToBounds([[z.lat_min, z.lon_min], [z.lat_max, z.lon_max]], { maxZoom: 11, duration: 0.8 }))
          .addTo(zonesLayer);
      }
    }
  }
  map.on("zoomend", renderZonesLayer);
  async function refreshZones() {
    try {
      zones = await api("/api/congestion?min_level=dense");
      renderZonesLayer();
      const flagged = zones.filter((z) => z.level !== "dense");
      $("#badge-zones").textContent = flagged.length ? fmt.format(flagged.length) : "";
      const feed = $("#zone-feed");
      feed.innerHTML = "";
      if (!flagged.length) feed.append(el("li", { class: "empty" }, "Aucune zone de congestion ou de ralentissement."));
      for (const z of flagged) {
        const lat = (z.lat_min + z.lat_max) / 2, lon = (z.lon_min + z.lon_max) / 2;
        feed.append(el("li", {
          class: z.level === "congestion" ? "sev-critical" : "sev-warning",
          onclick: () => map.flyToBounds([[z.lat_min, z.lon_min], [z.lat_max, z.lon_max]], { maxZoom: 11, duration: 0.8 }),
        },
        el("div", { class: "f-top" }, el("span", { class: "f-kind" }, `${LEVEL[z.level].label}${z.place ? ` · ${z.place}` : ""}`), el("span", { class: "mono" }, `${lat.toFixed(2)}, ${lon.toFixed(2)}`)),
        el("div", { class: "f-msg" }, `${z.n_vessels} navires dont ${z.n_waiting} cargos en attente · ${Number(z.mean_sog).toFixed(1)} nd en moyenne${z.baseline_sog ? ` (normale ${Number(z.baseline_sog).toFixed(1)} nd)` : ""}`)));
      }
    } catch { /* réessai au prochain cycle */ }
  }

  // ---------------------------------------------------------------- statistiques
  function bars(container, rows, colorOf) {
    const box = $(container);
    box.innerHTML = "";
    const max = Math.max(1, ...rows.map((r) => r.n));
    for (const r of rows) {
      box.append(el("div", { class: "bar-row" },
        el("span", { class: "bar-label", title: r.label }, r.label),
        el("span", { class: "bar-track" }, el("span", { class: "bar-fill", style: `width:${(100 * r.n) / max}%;background:${colorOf(r)}` })),
        el("span", { class: "bar-value" }, fmt.format(r.n))));
    }
    if (!rows.length) box.append(el("p", { class: "muted" }, "Pas encore de données."));
  }

  function renderThroughput(rows) {
    const box = $("#chart-throughput");
    if (rows.length < 2) { box.innerHTML = '<p class="muted">Collecte en cours…</p>'; return; }
    const W = Math.max(200, box.clientWidth || 320), H = 64;
    const vmax = Math.max(1, ...rows.map((r) => r.positions));
    const x = (i) => (i / (rows.length - 1)) * W;
    const y = (v) => H - 4 - (v / vmax) * (H - 10);
    const d = rows.map((r, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(r.positions).toFixed(1)}`).join("");
    const last = rows[rows.length - 1];
    box.innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Positions ingérées par minute">
      <path d="${d}L${W},${H}L0,${H}Z" fill="${cssVar("--info")}" opacity=".14"/>
      <path d="${d}" fill="none" stroke="${cssVar("--info")}" stroke-width="1.6"/></svg>
      <div class="spark-meta"><span>${new Date(rows[0].ts * 1000).toLocaleTimeString("fr-FR", { hour: "2-digit", minute: "2-digit" })}</span>
      <span>dernier : <b class="mono">${fmt.format(last.positions)}</b> pos.${last.lag_s !== null ? ` · latence ${Number(last.lag_s).toFixed(1)} s` : ""}</span></div>`;
  }

  async function refreshStats() {
    try {
      const s = await api("/api/stats");
      const k = s.kpi;
      $("#kpi-active").textContent = fmt.format(k.active);
      $("#kpi-moving").textContent = fmt.format(k.moving);
      $("#kpi-anchored").textContent = fmt.format(k.anchored);
      $("#kpi-delayed").textContent = fmt.format(k.delayed);
      $("#kpi-zones").textContent = fmt.format(k.congested_zones);
      $("#kpi-alerts").textContent = fmt.format(k.alerts_1h);
      bars("#chart-categories", s.by_category, (r) => catColors[r.code] || catColors.other);
      const kindColor = { DELAY: "--warn", CONGESTION: "--crit", SLOWDOWN: "--warn", POSITION_JUMP: "--crit", AIS_GAP: "--info", UNEXPECTED_STOP: "--info" };
      bars("#chart-anomalies", s.anomalies_24h.map((a) => ({ ...a, label: a.label })), (r) => cssVar(kindColor[r.kind] || "--muted"));
      renderThroughput(s.throughput);
      state.lastPosition = k.last_position;
    } catch { /* réessai */ }
  }

  async function refreshMeta() {
    try {
      const m = await api("/api/meta");
      renderCategoryChips(m.categories);
      renderCountries(m.countries);
      if (!state.kindsRendered) { renderAlertKinds(m.anomaly_kinds); state.kindsRendered = true; }
      $("#source-badge").textContent = m.source === "aisstream" ? "aisstream.io · mondial" : "Digitraffic · Baltique";
    } catch { /* réessai */ }
  }

  async function refreshVessels() {
    try {
      const data = await api(`/api/vessels?${queryString()}`);
      state.vessels = data.vessels;
      vesselLayer.setData(data.vessels);
      $("#filter-count").textContent = `${fmt.format(data.count)} navire${data.count > 1 ? "s" : ""} affiché${data.count > 1 ? "s" : ""}`;
      state.lastOk = Date.now();
      if (state.follow) centerOnSelected();
    } catch { /* indicateur passe en hors ligne */ }
    updateLive();
  }

  function updateLive() {
    const dot = $("#live-dot"), text = $("#live-text");
    const ageApi = (Date.now() - state.lastOk) / 1000;
    dot.className = "live-dot";
    if (!state.lastOk || ageApi > 90) { dot.classList.add("off"); text.textContent = "API injoignable"; return; }
    const lastPos = state.lastPosition ? toMs(state.lastPosition) : 0;
    const dataAge = (Date.now() - lastPos) / 1000;
    dot.classList.add(dataAge < 180 ? "on" : "stale");
    text.textContent = lastPos ? `En direct · dernière position ${ago(state.lastPosition)}` : "En attente des premières positions…";
  }

  // ------------------------------------------------- IA : bulletin et assistant
  // Texte du LLM -> paragraphes et listes ; les MMSI (9 chiffres) deviennent des liens vers la fiche navire.
  function linkify(text) {
    const parts = [];
    let last = 0;
    for (const m of text.matchAll(/\b\d{9}\b/g)) {
      if (m.index > last) parts.push(text.slice(last, m.index));
      const mmsi = Number(m[0]);
      parts.push(el("button", { class: "mmsi-link", type: "button", title: "Voir la fiche du navire", onclick: () => selectVessel(mmsi, true) }, m[0]));
      last = m.index + m[0].length;
    }
    if (last < text.length) parts.push(text.slice(last));
    return parts;
  }
  function richText(text) {
    const frag = document.createDocumentFragment();
    let list = null;
    // Les petits modèles ajoutent du markdown (gras, titres) malgré la consigne : on le retire
    for (const raw of String(text || "").replace(/\*\*/g, "").replace(/^#+\s*/gm, "").split("\n")) {
      const line = raw.trim();
      if (!line) { list = null; continue; }
      const bullet = line.match(/^(?:[-•*]|\d+[.)])\s+(.*)$/);
      if (bullet) {
        if (!list) { list = el("ul"); frag.append(list); }
        list.append(el("li", {}, linkify(bullet[1])));
      } else {
        list = null;
        frag.append(el("p", {}, linkify(line)));
      }
    }
    return frag;
  }

  const genBadge = (b) => el("span", { class: `gen-badge ${b.generator}` },
    b.generator === "llm" ? `LLM · ${b.model}` : "gabarit (LLM indisponible)");

  async function refreshBriefing() {
    try {
      const rows = await api("/api/briefings?limit=8");
      const box = $("#briefing-current");
      if (!rows.length) return;
      const [cur, ...older] = rows;
      if (cur.id !== state.briefingId) {
        box.innerHTML = "";
        box.append(richText(cur.text), el("div", { class: "briefing-meta" },
          genBadge(cur), el("span", {}, `${fmtDate(cur.ts)} · ${ago(cur.ts)}`),
          cur.duration_s ? el("span", {}, `rédigé en ${Number(cur.duration_s).toFixed(0)} s`) : null,
          el("span", {}, `période : ${cur.window_min} min`)));
        const feed = $("#briefing-feed");
        feed.innerHTML = "";
        for (const b of older) feed.append(el("li", {}, el("div", { class: "f-top" }, el("span", {}, fmtDate(b.ts)), genBadge(b)), b.text));
        if (state.briefingId && store.get("vigie.tab", "") !== "briefing") $("#badge-briefing").hidden = false;
        state.briefingId = cur.id;
      }
    } catch { /* réessai */ }
  }

  async function refreshLlmStatus() {
    const box = $("#llm-status");
    try {
      const s = await api("/api/llm/status");
      state.llmOk = s.available;
      box.className = `llm-status ${s.available ? "ok" : "off"}`;
      box.textContent = !s.enabled ? "IA désactivée (LLM_URL vide)"
        : s.available ? `Modèle ${s.model} prêt (exécution locale)`
        : `Modèle ${s.model} indisponible : démarrage ou téléchargement en cours`;
    } catch { box.className = "llm-status off"; box.textContent = "API injoignable"; }
  }

  const chatLog = $("#chat-log"), chatInput = $("#chat-input"), chatSend = $("#chat-send");
  state.chat = [];
  function addMsg(kind, content, extra) {
    const node = el("div", { class: `msg ${kind}` });
    if (kind === "bot") node.append(richText(content)); else node.append(content);
    if (extra) node.append(extra);
    chatLog.append(node);
    chatLog.scrollTop = chatLog.scrollHeight;
    return node;
  }
  const TOOL_LABELS = { search_vessels: "recherche navires", vessel_details: "fiche navire", congestion_zones: "zones",
    recent_alerts: "alertes", traffic_stats: "statistiques", latest_briefing: "bulletin" };

  async function askAssistant(question) {
    question = question.trim();
    if (question.length < 2 || chatSend.disabled) return;
    addMsg("user", question);
    chatInput.value = "";
    chatSend.disabled = true;
    const started = Date.now();
    const bubble = addMsg("bot pending", "Le modèle réfléchit…");
    const status = el("span", { class: "tools" });
    const used = [];
    let text = "", answer = null;
    const setStatus = (msg) => { status.textContent = msg; if (!status.isConnected) bubble.append(status); };
    const render = () => { bubble.className = "msg bot"; bubble.replaceChildren(richText(text)); bubble.append(status); chatLog.scrollTop = chatLog.scrollHeight; };
    const tick = setInterval(() => { if (!text) setStatus(`${used.length ? `Consulté : ${used.join(", ")} · ` : ""}${Math.round((Date.now() - started) / 1000)} s`); }, 1000);
    try {
      const r = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/x-ndjson" },
        body: JSON.stringify({ question, history: state.chat.slice(-4) }),
      });
      if (!r.ok) {
        const data = await r.json().catch(() => ({}));
        bubble.remove(); addMsg("err", data.detail || `Erreur ${r.status}`); return;
      }
      // Lecture du flux NDJSON : une ligne JSON par événement
      const reader = r.body.getReader(), dec = new TextDecoder();
      let buf = "";
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let nl;
        while ((nl = buf.indexOf("\n")) >= 0) {
          const line = buf.slice(0, nl).trim(); buf = buf.slice(nl + 1);
          if (!line) continue;
          const ev = JSON.parse(line);
          if (ev.type === "tool") { const l = TOOL_LABELS[ev.name] || ev.name; if (!used.includes(l)) used.push(l); setStatus(`Consulte : ${used.join(", ")}…`); }
          else if (ev.type === "token") { text += ev.text; render(); }
          else if (ev.type === "reset") { text = ""; }
          else if (ev.type === "done") { answer = ev.answer; text = ev.answer; }
          else if (ev.type === "error") { throw new Error(ev.message); }
        }
      }
      if (answer === null) throw new Error("Réponse interrompue.");
      render();
      setStatus(`${used.length ? `Sources : ${used.join(", ")} · ` : ""}${Math.round((Date.now() - started) / 1000)} s`);
      state.chat.push({ role: "user", content: question }, { role: "assistant", content: answer });
    } catch (e) {
      bubble.remove();
      addMsg("err", e.message && !/fetch/i.test(e.message) ? e.message : "Impossible de joindre l'API.");
    } finally {
      clearInterval(tick);
      chatSend.disabled = false;
      chatInput.focus();
    }
  }
  $("#chat-form").addEventListener("submit", (e) => { e.preventDefault(); askAssistant(chatInput.value); });
  chatInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); askAssistant(chatInput.value); }
  });
  for (const s of ["Résume la situation actuelle", "Quelles zones sont congestionnées ?", "Quels navires sont en retard ?", "Y a-t-il des arrêts en mer suspects ?"]) {
    $("#chat-suggestions").append(el("button", { class: "chip", type: "button", onclick: () => askAssistant(s) }, s));
  }

  // ------------------------------------------------------------------ onglets
  document.querySelectorAll(".tabs button").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll(".tabs button").forEach((x) => x.classList.toggle("active", x === b));
    document.querySelectorAll(".tab-panel").forEach((p) => { p.hidden = p.id !== `tab-${b.dataset.tab}`; });
    store.set("vigie.tab", b.dataset.tab);
    if (b.dataset.tab === "briefing") $("#badge-briefing").hidden = true;
  }));
  const savedTab = store.get("vigie.tab", "alerts");
  document.querySelector(`.tabs button[data-tab="${savedTab}"]`)?.click();

  // ------------------------------------------------------------------- démarrage
  commercial.checked = state.filters.commercial;
  syncSegments();
  renderLegend();
  refreshMeta();
  refreshStats().then(refreshVessels);
  refreshAlerts();
  refreshZones();
  refreshBriefing();
  refreshLlmStatus();
  setInterval(refreshVessels, 15000);
  setInterval(refreshBriefing, 60000);
  setInterval(refreshLlmStatus, 30000);
  setInterval(refreshStats, 15000);
  setInterval(() => { refreshAlerts(); refreshZones(); }, 30000);
  setInterval(refreshMeta, 60000);
  setInterval(() => { if (state.selected) { loadDetail(); loadTrack(); } }, 30000);
  setInterval(updateLive, 5000);
})();
