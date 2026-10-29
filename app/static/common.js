// Shared helpers for every page: API wrapper with DB trace capture, nav, toasts, JSON view.
const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const money = v => '₹' + Number(v || 0).toLocaleString('en-IN');
const STORE_LABEL = { redis: 'Redis', mongo: 'MongoDB', neo4j: 'Neo4j' };
const CATEGORY_ICON = { footwear: '👟', apparel: '👕', bags: '🎒', accessories: '☕', laptops: '💻', electronics: '🎧', outdoor: '⛺' };
const storeBadge = s => `<span class="badge ${esc(s)}">${esc(STORE_LABEL[s] || s)}</span>`;
const statusBadge = s => `<span class="badge ${esc(s)}">${esc(s)}</span>`;
const fmtTime = iso => iso ? new Date(iso).toLocaleTimeString() : '—';
const fmtDateTime = iso => iso ? new Date(iso).toLocaleString() : '—';

function formatDetail(data) {
  if (!data) return '';
  if (typeof data === 'string') return data;
  if (typeof data.detail === 'string') return data.detail;
  if (Array.isArray(data.detail)) return data.detail.map(d => `${(d.loc || []).slice(1).join('.') || 'body'}: ${d.msg}`).join('; ');
  return JSON.stringify(data);
}

async function api(path, { method = 'GET', body, headers = {}, quiet = false } = {}) {
  const opts = { method, headers: { ...headers }, credentials: 'same-origin' };
  if (body !== undefined) { opts.headers['Content-Type'] = 'application/json'; opts.body = JSON.stringify(body); }
  const started = performance.now();
  const res = await fetch(path, opts);
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  let ops = [];
  try { ops = JSON.parse(res.headers.get('X-DB-Trace') || '[]'); } catch { ops = []; }
  const entry = { method, path, status: res.status, ops, cid: res.headers.get('X-Correlation-ID'),
    ms: Number(res.headers.get('X-Response-Time-ms')) || Math.round(performance.now() - started), at: new Date() };
  if (!quiet) window.dispatchEvent(new CustomEvent('dbtrace', { detail: entry }));
  if (!res.ok) {
    const err = new Error(formatDetail(data) || res.statusText);
    err.status = res.status; err.data = data;
    throw err;
  }
  return data;
}

function toast(message, kind = 'info', ms = 4200) {
  let wrap = $('.toast-wrap');
  if (!wrap) { wrap = document.createElement('div'); wrap.className = 'toast-wrap'; document.body.appendChild(wrap); }
  const t = document.createElement('div');
  t.className = `toast ${kind}`;
  t.textContent = message;
  wrap.appendChild(t);
  setTimeout(() => t.remove(), ms);
}

function jsonHtml(value) {
  const json = JSON.stringify(value, null, 2) ?? 'null';
  return esc(json).replace(/(&quot;(?:\\.|[^&]|&(?!quot;))*?&quot;)(\s*:)?|\b(true|false)\b|\bnull\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g,
    (m, str, colon, bool) => {
      if (str) return colon ? `<span class="j-key">${str}</span>${colon}` : `<span class="j-str">${str}</span>`;
      if (bool) return `<span class="j-bool">${m}</span>`;
      if (m === 'null') return `<span class="j-null">${m}</span>`;
      return `<span class="j-num">${m}</span>`;
    });
}
const jsonBlock = v => `<pre class="code">${jsonHtml(v)}</pre>`;

function renderNav(active) {
  const host = location.hostname;
  const links = [['/', 'Shop', 'shop'], ['/admin', 'Admin', 'admin'], ['/dashboard', 'Dashboard', 'dashboard'], ['/explorer', 'DB Explorer', 'explorer']];
  const bar = document.createElement('header');
  bar.className = 'topbar';
  bar.innerHTML = `
    <div class="brand">Polyglot Commerce <span>· Redis · MongoDB · Neo4j</span></div>
    <nav class="nav">${links.map(([href, label, key]) => `<a href="${href}" class="${key === active ? 'active' : ''}">${label}</a>`).join('')}
      <a href="/docs" target="_blank" rel="noopener">API docs</a></nav>
    <div class="spacer"></div>
    <details class="ext"><summary>Database UIs ▾</summary><div class="menu">
      <a href="http://${host}:7474/browser/" target="_blank" rel="noopener"><b>Neo4j Browser</b><small>:7474 · user neo4j / review2password</small></a>
      <a href="http://${host}:8081" target="_blank" rel="noopener"><b>Mongo Express</b><small>:8081 · user admin / admin123</small></a>
      <a href="http://${host}:8082" target="_blank" rel="noopener"><b>Redis Commander</b><small>:8082 · no login</small></a>
    </div></details>
    <div class="health" id="health-pills">
      <span class="pill" data-store="redis"><span class="dot"></span>Redis</span>
      <span class="pill" data-store="mongo"><span class="dot"></span>MongoDB</span>
      <span class="pill" data-store="neo4j"><span class="dot"></span>Neo4j</span>
    </div>`;
  document.body.prepend(bar);
  refreshHealth();
  setInterval(refreshHealth, 8000);
}

async function refreshHealth() {
  let h = null;
  try { h = await api('/api/health', { quiet: true }); } catch { h = null; }
  $$('#health-pills .pill').forEach(p => {
    const s = h?.stores?.[p.dataset.store];
    p.classList.toggle('ok', !!s?.ok);
    p.classList.toggle('down', !s?.ok);
    p.title = s ? (s.ok ? `${p.dataset.store} ok · ${s.ms} ms · ${s.version || ''}` : s.error) : 'API unreachable';
  });
  window.dispatchEvent(new CustomEvent('health', { detail: h }));
  return h;
}

// Trace panel: shows, for every API call made by this page, which database did what.
function mountTracePanel(el, max = 40) {
  const items = [];
  const draw = () => {
    el.innerHTML = items.length ? items.map(t => `
      <div class="trace-item">
        <div class="trace-head"><span class="method">${esc(t.method)}</span><span class="mono">${esc(t.path)}</span>
          <span class="${t.status < 400 ? 'status-ok' : 'status-bad'}">${t.status}</span>
          <span class="muted">${Number(t.ms).toFixed(1)} ms</span>
          <span class="muted small">corr ${esc(t.cid || '—')} · ${t.at.toLocaleTimeString()}</span></div>
        <ul class="trace-ops">${t.ops.map(o => `<li>${storeBadge(o.store)}<span class="op">${esc(o.op)}</span>
          <span class="muted small mono">${esc(o.detail)}</span>${o.error ? `<span class="badge bad">${esc(o.error)}</span>` : ''}
          <span class="ms">${o.ms.toFixed(2)} ms</span></li>`).join('') || '<li class="muted">no database operation</li>'}</ul>
      </div>`).join('') : '<p class="muted">Interact with the page: every request and the database operations behind it appear here.</p>';
  };
  window.addEventListener('dbtrace', e => { items.unshift(e.detail); items.length = Math.min(items.length, max); draw(); });
  draw();
  return { clear() { items.length = 0; draw(); } };
}

function newIdempotencyKey() {
  const id = (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(16).slice(2) + Date.now().toString(16));
  return 'ui-' + id.replace(/[^A-Za-z0-9-]/g, '');
}

function debounce(fn, ms = 300) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }
function attrSummary(attrs, n = 3) {
  return Object.entries(attrs || {}).slice(0, n).map(([k, v]) => `${k}: ${typeof v === 'boolean' ? (v ? 'yes' : 'no') : v}`).join(' · ');
}
