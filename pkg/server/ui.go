package server

import "net/http"

func (s *Server) handleUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(uiHTML))
}

const uiHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ComputeHarbor Status &amp; Spot Savings Console</title>
<style>
  :root {
    --bg: #0f172a;
    --surface: #1e293b;
    --surface-alt: #0b1220;
    --border: #334155;
    --text: #f8fafc;
    --muted: #94a3b8;
    --accent: #38bdf8;
    --green: #22c55e;
    --amber: #f59e0b;
    --red: #ef4444;
  }
  * { box-sizing: border-box; }
  .skip-link {
    position: absolute;
    left: -9999px;
    top: 0;
    background: var(--accent);
    color: #082f49;
    padding: 8px 14px;
    border-radius: 6px;
    font-size: 0.875rem;
    font-weight: 600;
    text-decoration: none;
    z-index: 100;
  }
  .skip-link:focus {
    position: fixed;
    left: 16px;
    top: 16px;
  }
  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }
  body {
    margin: 0;
    padding: 24px;
    background: var(--bg);
    color: var(--text);
    font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    line-height: 1.5;
  }
  .container { max-width: 1200px; margin: 0 auto; }
  header {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    padding-bottom: 20px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 24px;
  }
  h1 { margin: 0; font-size: 1.35rem; font-weight: 600; letter-spacing: -0.01em; }
  .subtitle { color: var(--muted); font-size: 0.875rem; margin-top: 2px; }
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px;
  }
  .field {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }
  label { font-size: 0.8rem; color: var(--muted); font-weight: 500; }
  input[type="text"],
  input[type="password"] {
    background: var(--surface);
    color: var(--text);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 7px 11px;
    font-size: 0.875rem;
    min-width: 160px;
  }
  a:focus-visible,
  button:focus-visible,
  input:focus-visible,
  [tabindex]:focus-visible {
    outline: 2px solid #38bdf8;
    outline-offset: 2px;
  }
  button {
    background: var(--accent);
    color: #082f49;
    border: none;
    border-radius: 6px;
    padding: 8px 14px;
    font-size: 0.875rem;
    font-weight: 600;
    cursor: pointer;
  }
  button:hover { filter: brightness(1.08); }
  button:disabled { opacity: 0.6; cursor: wait; }
  .badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 600;
    background: var(--surface);
    border: 1px solid var(--border);
  }
  .badge.ok { color: var(--green); border-color: rgba(34, 197, 94, 0.4); }
  .badge.err { color: var(--red); border-color: rgba(239, 68, 68, 0.4); }
  .kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 18px;
  }
  .kpi-label { font-size: 0.78rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
  .kpi-value { font-size: 1.65rem; font-weight: 700; margin-top: 6px; }
  .kpi-sub { font-size: 0.8rem; color: var(--muted); margin-top: 4px; }
  .panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 10px;
    margin-bottom: 20px;
    overflow: hidden;
  }
  .panel-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 14px 18px;
    border-bottom: 1px solid var(--border);
  }
  .panel-title { font-size: 0.98rem; font-weight: 600; margin: 0; }
  .panel-meta { font-size: 0.78rem; color: var(--muted); }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
  th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--border); white-space: nowrap; }
  th { color: var(--muted); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; background: var(--surface-alt); }
  tr:last-child td { border-bottom: none; }
  .num { text-align: right; font-variant-numeric: tabular-nums; }
  .status-pill {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 9999px;
    font-size: 0.74rem;
    font-weight: 600;
    background: var(--surface-alt);
    border: 1px solid var(--border);
  }
  .status-running, .status-completed { color: var(--green); }
  .status-preempted, .status-pending { color: var(--amber); }
  .status-failed, .status-cancelled { color: var(--red); }
  pre.metrics {
    margin: 0;
    padding: 16px 18px;
    background: var(--surface-alt);
    color: #cbd5e1;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 0.78rem;
    overflow-x: auto;
    max-height: 320px;
  }
  .empty { padding: 20px 18px; color: var(--muted); font-size: 0.875rem; }
  .error-text { color: var(--red); }
  .auth-banner {
    display: none;
    background: rgba(239, 68, 68, 0.12);
    border: 1px solid rgba(239, 68, 68, 0.45);
    color: #fecaca;
    border-radius: 8px;
    padding: 10px 14px;
    margin-bottom: 18px;
    font-size: 0.875rem;
    font-weight: 500;
  }
  .auth-banner.visible { display: block; }
  .footer {
    margin-top: 24px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
    color: var(--muted);
    font-size: 0.8rem;
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    gap: 8px;
  }
  @media (max-width: 640px) {
    body { padding: 16px; }
    header { flex-direction: column; align-items: stretch; }
    .controls { flex-direction: column; align-items: stretch; }
    .field { flex-direction: column; align-items: stretch; }
    input[type="text"],
    input[type="password"],
    button { width: 100%; min-width: 0; }
    .panel-header { flex-direction: column; align-items: flex-start; gap: 4px; }
    .kpi-grid { grid-template-columns: 1fr; }
  }
</style>
</head>
<body>
<a class="skip-link" href="#main-content">Skip to main content</a>
<div class="container">
  <header>
    <div>
      <h1>ComputeHarbor Status &amp; Spot Savings Console</h1>
      <div class="subtitle">Control plane health, regional spot fleet rankings, jobs, and org savings</div>
    </div>
    <div class="controls" role="search" aria-label="Console filters and authentication">
      <span id="health-badge" class="badge" role="status" aria-live="polite">Checking...</span>
      <div class="field">
        <label for="orgInput">X-Org-ID</label>
        <input id="orgInput" type="text" value="org-demo" autocomplete="off">
      </div>
      <div class="field">
        <label for="apiKeyInput">API Key</label>
        <input id="apiKeyInput" type="password" placeholder="Optional Bearer API key" autocomplete="off">
      </div>
      <button id="refresh-btn" type="button">Refresh</button>
    </div>
  </header>

  <main id="main-content" tabindex="-1">
    <div id="auth-error-banner" class="auth-banner" role="alert" aria-live="polite">API key required or invalid</div>

    <section class="kpi-grid" aria-label="Key performance indicators" aria-live="polite">
      <div class="card">
        <div class="kpi-label">Net Spot Savings</div>
        <div id="kpi-savings" class="kpi-value">--</div>
        <div id="kpi-savings-sub" class="kpi-sub">Waiting for billing summary</div>
      </div>
      <div class="card">
        <div class="kpi-label">Est. Spot vs On-Demand</div>
        <div id="kpi-spend" class="kpi-value">--</div>
        <div id="kpi-spend-sub" class="kpi-sub">--</div>
      </div>
      <div class="card">
        <div class="kpi-label">Organization Jobs</div>
        <div id="kpi-jobs" class="kpi-value">--</div>
        <div id="kpi-jobs-sub" class="kpi-sub">--</div>
      </div>
      <div class="card">
        <div class="kpi-label">Top Spot Pool (24GB+ VRAM)</div>
        <div id="kpi-top-pool" class="kpi-value">--</div>
        <div id="kpi-top-pool-sub" class="kpi-sub">--</div>
      </div>
    </section>

    <section class="panel" aria-labelledby="fleet-heading">
      <div class="panel-header">
        <h2 id="fleet-heading" class="panel-title">Spot Fleet Rankings (min_vram=24)</h2>
        <span id="fleet-meta" class="panel-meta">/v1/fleet/rank?min_vram=24</span>
      </div>
      <div id="fleet-body" class="table-wrap" aria-live="polite">
        <table aria-label="Spot Fleet Rankings">
          <thead>
            <tr>
              <th scope="col">GPU</th>
              <th scope="col">Provider</th>
              <th scope="col">Region</th>
              <th scope="col" class="num">VRAM</th>
              <th scope="col" class="num">Spot/hr</th>
              <th scope="col" class="num">On-Demand/hr</th>
              <th scope="col" class="num">Effective/hr</th>
              <th scope="col" class="num">Net Savings</th>
              <th scope="col" class="num">Availability</th>
              <th scope="col" class="num">Score</th>
            </tr>
          </thead>
          <tbody>
            <tr><td colspan="10" class="empty">Loading fleet rankings...</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel" aria-labelledby="jobs-heading">
      <div class="panel-header">
        <h2 id="jobs-heading" class="panel-title">Organization Jobs</h2>
        <span id="jobs-meta" class="panel-meta">/v1/jobs</span>
      </div>
      <div id="jobs-body" class="table-wrap" aria-live="polite">
        <table aria-label="Organization Jobs">
          <thead>
            <tr>
              <th scope="col">Job ID</th>
              <th scope="col">GPU</th>
              <th scope="col">Status</th>
              <th scope="col">Worker</th>
              <th scope="col" class="num">Progress</th>
              <th scope="col" class="num">Preemptions</th>
              <th scope="col" class="num">Last Recovery</th>
              <th scope="col">Updated</th>
            </tr>
          </thead>
          <tbody>
            <tr><td colspan="8" class="empty">Loading jobs...</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="panel" aria-labelledby="metrics-heading">
      <div class="panel-header">
        <h2 id="metrics-heading" class="panel-title">Control Plane Metrics</h2>
        <span id="metrics-meta" class="panel-meta">/metrics</span>
      </div>
      <pre id="metrics-body" class="metrics" aria-live="polite">Loading metrics...</pre>
    </section>
  </main>

  <footer class="footer">
    <span>ComputeHarbor Control Plane Console</span>
    <span>Keyboard: Tab to navigate controls · Enter to refresh</span>
  </footer>
</div>

<script>
(function () {
  var orgInput = document.getElementById("orgInput");
  var apiKeyInput = document.getElementById("apiKeyInput");
  var authBanner = document.getElementById("auth-error-banner");
  var refreshBtn = document.getElementById("refresh-btn");
  var healthBadge = document.getElementById("health-badge");
  var API_KEY_STORAGE_KEY = "ch_api_key";

  try {
    var savedKey = sessionStorage.getItem(API_KEY_STORAGE_KEY);
    if (savedKey) {
      apiKeyInput.value = savedKey;
    }
  } catch (e) {}

  function persistApiKey() {
    try {
      var val = (apiKeyInput.value || "").trim();
      if (val) {
        sessionStorage.setItem(API_KEY_STORAGE_KEY, val);
      } else {
        sessionStorage.removeItem(API_KEY_STORAGE_KEY);
      }
    } catch (e) {}
  }

  function buildHeaders(extra) {
    var headers = {};
    if (extra) {
      Object.keys(extra).forEach(function (k) { headers[k] = extra[k]; });
    }
    var token = (apiKeyInput.value || "").trim();
    if (token) {
      headers["Authorization"] = "Bearer " + token;
    }
    return headers;
  }

  function showAuthError() {
    authBanner.textContent = "API key required or invalid";
    authBanner.className = "auth-banner visible";
  }

  function clearAuthError() {
    authBanner.className = "auth-banner";
  }

  async function fetchJSON(url, extraHeaders) {
    var res = await fetch(url, { headers: buildHeaders(extraHeaders) });
    if (res.status === 401) {
      showAuthError();
    }
    var data = await res.json();
    if (!res.ok) {
      throw new Error(data.error || ("HTTP " + res.status));
    }
    return data;
  }

  function usd(v) {
    return "$" + Number(v || 0).toFixed(2);
  }
  function pct(v) {
    return (Number(v || 0) * 100).toFixed(1) + "%";
  }
  function clearAndMsg(el, text, isErr) {
    el.textContent = "";
    var d = document.createElement("div");
    d.className = isErr ? "empty error-text" : "empty";
    d.textContent = text;
    el.appendChild(d);
  }
  function buildTable(headers, rows, numericCols) {
    var table = document.createElement("table");
    var thead = document.createElement("thead");
    var hr = document.createElement("tr");
    headers.forEach(function (h, i) {
      var th = document.createElement("th");
      th.setAttribute("scope", "col");
      th.textContent = h;
      if (numericCols[i]) th.className = "num";
      hr.appendChild(th);
    });
    thead.appendChild(hr);
    table.appendChild(thead);

    var tbody = document.createElement("tbody");
    rows.forEach(function (cells) {
      var tr = document.createElement("tr");
      cells.forEach(function (cell, i) {
        var td = document.createElement("td");
        if (numericCols[i]) td.className = "num";
        if (cell && cell.nodeType) {
          td.appendChild(cell);
        } else {
          td.textContent = String(cell);
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    return table;
  }

  async function fetchHealth() {
    try {
      var res = await fetch("/healthz", { headers: buildHeaders() });
      if (res.status === 401) {
        showAuthError();
      }
      var data = await res.json();
      if (res.ok && data.status === "ok") {
        healthBadge.textContent = "Health: OK";
        healthBadge.className = "badge ok";
      } else {
        healthBadge.textContent = "Health: Degraded";
        healthBadge.className = "badge err";
      }
    } catch (err) {
      healthBadge.textContent = "Health: Unreachable";
      healthBadge.className = "badge err";
    }
  }

  async function fetchBilling(orgId) {
    var savingsEl = document.getElementById("kpi-savings");
    var savingsSub = document.getElementById("kpi-savings-sub");
    var spendEl = document.getElementById("kpi-spend");
    var spendSub = document.getElementById("kpi-spend-sub");
    try {
      var data = await fetchJSON("/v1/billing/summary", { "X-Org-ID": orgId });
      savingsEl.textContent = usd(data.net_savings_usd);
      savingsSub.textContent = pct(data.net_savings_fraction) + " net savings (" + usd(data.platform_fee_usd) + " fee)";
      spendEl.textContent = usd(data.estimated_spot_usd);
      spendSub.textContent = "vs " + usd(data.estimated_on_demand_usd) + " on-demand";
      document.getElementById("kpi-jobs").textContent = String(data.total_jobs || 0);
      document.getElementById("kpi-jobs-sub").textContent =
        (data.completed_jobs || 0) + " completed · " + (data.total_steps || 0) + " steps · " + (data.total_preemptions || 0) + " preemptions";
    } catch (err) {
      savingsEl.textContent = "--";
      savingsSub.textContent = "Error: " + err.message;
      spendEl.textContent = "--";
      spendSub.textContent = "--";
    }
  }

  async function fetchFleet() {
    var container = document.getElementById("fleet-body");
    var topPool = document.getElementById("kpi-top-pool");
    var topPoolSub = document.getElementById("kpi-top-pool-sub");
    try {
      var data = await fetchJSON("/v1/fleet/rank?min_vram=24");
      if (!Array.isArray(data) || data.length === 0) {
        topPool.textContent = "None";
        topPoolSub.textContent = "No matching pools";
        clearAndMsg(container, "No fleet pools matched min_vram=24.", false);
        return;
      }
      var best = data[0];
      topPool.textContent = best.pool.gpu + " (" + best.pool.provider + ")";
      topPoolSub.textContent = best.pool.region + " · " + usd(best.effective_hourly_usd) + "/hr · " + pct(best.net_savings_fraction) + " saved";

      var rows = data.map(function (item) {
        var p = item.pool;
        return [
          p.gpu,
          p.provider,
          p.region,
          p.vram_gb + " GB",
          usd(p.spot_hourly_usd),
          usd(p.on_demand_hourly_usd),
          usd(item.effective_hourly_usd),
          pct(item.net_savings_fraction),
          pct(p.availability_score),
          Number(item.composite_score || 0).toFixed(3)
        ];
      });
      container.textContent = "";
      container.appendChild(buildTable(
        ["GPU", "Provider", "Region", "VRAM", "Spot/hr", "On-Demand/hr", "Effective/hr", "Net Savings", "Availability", "Score"],
        rows,
        [false, false, false, true, true, true, true, true, true, true]
      ));
    } catch (err) {
      topPool.textContent = "--";
      topPoolSub.textContent = "Error: " + err.message;
      clearAndMsg(container, "Failed to load fleet rankings: " + err.message, true);
    }
  }

  async function fetchJobs(orgId) {
    var container = document.getElementById("jobs-body");
    try {
      var data = await fetchJSON("/v1/jobs", { "X-Org-ID": orgId });
      if (!Array.isArray(data) || data.length === 0) {
        clearAndMsg(container, "No jobs found for organization " + orgId + ".", false);
        return;
      }
      var rows = data.map(function (job) {
        var pill = document.createElement("span");
        pill.className = "status-pill status-" + String(job.status || "").toLowerCase();
        pill.textContent = job.status || "unknown";
        return [
          job.job_id,
          job.gpu,
          pill,
          job.worker_node_id || "-",
          (job.current_step || 0) + " / " + (job.target_steps || 0),
          (job.preemption_count || 0) + " / " + (job.max_preemptions || 0),
          job.last_recovery_dur_ms ? (job.last_recovery_dur_ms + " ms") : "-",
          job.updated_at ? new Date(job.updated_at).toLocaleString() : "-"
        ];
      });
      container.textContent = "";
      container.appendChild(buildTable(
        ["Job ID", "GPU", "Status", "Worker", "Progress", "Preemptions", "Last Recovery", "Updated"],
        rows,
        [false, false, false, false, true, true, true, false]
      ));
    } catch (err) {
      clearAndMsg(container, "Failed to load jobs: " + err.message, true);
    }
  }

  async function fetchMetrics() {
    var pre = document.getElementById("metrics-body");
    try {
      var res = await fetch("/metrics", { headers: buildHeaders() });
      if (res.status === 401) {
        showAuthError();
      }
      var text = await res.text();
      if (!res.ok) {
        throw new Error("HTTP " + res.status);
      }
      pre.textContent = text;
    } catch (err) {
      pre.textContent = "Failed to load /metrics: " + err.message;
    }
  }

  async function refreshAll() {
    var orgId = (orgInput.value || "").trim() || "org-demo";
    orgInput.value = orgId;
    persistApiKey();
    clearAuthError();
    refreshBtn.disabled = true;
    try {
      await Promise.all([
        fetchHealth(),
        fetchBilling(orgId),
        fetchFleet(),
        fetchJobs(orgId),
        fetchMetrics()
      ]);
    } finally {
      refreshBtn.disabled = false;
    }
  }

  refreshBtn.addEventListener("click", refreshAll);
  orgInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") refreshAll();
  });
  apiKeyInput.addEventListener("input", persistApiKey);
  apiKeyInput.addEventListener("keydown", function (e) {
    if (e.key === "Enter") refreshAll();
  });
  refreshAll();
})();
</script>
</body>
</html>
`
