/* Spark Control dashboard shell. Dumb on purpose: fetch /api with the
   bearer token from localStorage, render the view-model, long-press stop.
   The Python view-model (/api/ui) owns every state decision; this file only
   fetches, renders, routes between pages, and fires actions. */
"use strict";

const TOKEN_KEY = "sparkctl_token";
const RECIPE_KEY = "sparkctl_recipe"; // which recipe START targets (default below)
const DEFAULT_RECIPE = ""; // filled from the view-model (SPARKCTL_DEFAULT_RECIPE or first recipe)
const LONGPRESS_MS = 800; // PRD FR-2: stop requires >= 800 ms press
const REFRESH_MS = 5000;
const PAGES = ["home", "recipes", "jobs", "monitor"];

const $ = (id) => document.getElementById(id);
let lastVm = null; // last rendered view-model
let jobId = null; // job we follow while pending/running
let pendingAction = null; // recipe id while a start/stop POST is in flight
let timer = null;

function recipe() {
  const fallback =
    (lastVm && lastVm.default_recipe) ||
    (lastVm && lastVm.recipes && lastVm.recipes[0] && lastVm.recipes[0].recipe_id) ||
    DEFAULT_RECIPE;
  return localStorage.getItem(RECIPE_KEY) || fallback;
}

function token() {
  let t = localStorage.getItem(TOKEN_KEY);
  if (!t) {
    t = window.prompt("Paste the Spark Control bearer token (SPARKCTL_TOKEN):");
    if (t) localStorage.setItem(TOKEN_KEY, t);
  }
  return t;
}

async function api(path, options) {
  const headers = Object.assign(
    { Authorization: "Bearer " + token() },
    options && options.body ? { "Content-Type": "application/json" } : {},
    (options && options.headers) || {}
  );
  const resp = await fetch(path, Object.assign({}, options, { headers }));
  if (resp.status === 401) {
    localStorage.removeItem(TOKEN_KEY);
    token();
    throw Object.assign(new Error("unauthorized"), { code: "unauthorized" });
  }
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = (data && data.error) || {};
    throw Object.assign(new Error(err.detail || resp.statusText), { code: err.code });
  }
  return data;
}

/* ---------- routing ---------- */

function setPage(p) {
  if (!PAGES.includes(p)) p = "home";
  window.scrollTo(0, 0);
  document.querySelectorAll(".page").forEach((el) =>
    el.classList.toggle("hidden", el.id !== "page-" + p)
  );
  document.querySelectorAll(".tab").forEach((b) =>
    b.classList.toggle("active", b.dataset.page === p)
  );
  if (p === "jobs") loadJobs();
  if (location.hash !== "#" + p) history.replaceState(null, "", "#" + p);
}

function initRouting() {
  document.querySelectorAll(".tab").forEach((b) =>
    b.addEventListener("click", () => setPage(b.dataset.page))
  );
  setPage((location.hash || "#home").replace("#", ""));
  window.addEventListener("hashchange", () => setPage(location.hash.replace("#", "")));
}

/* ---------- rendering ---------- */

function render(vm) {
  lastVm = vm;
  const stateEl = $("state-line");
  stateEl.textContent = vm.state.toUpperCase();
  stateEl.className = "state state-" + vm.state;
  $("model-line").textContent = vm.model
    ? "Serving: " + vm.model
    : vm.state === "unreachable"
      ? "Cluster unreachable"
      : "";

  $("nodes").replaceChildren(
    ...vm.nodes.map((n) => {
      const li = document.createElement("li");
      const name = document.createElement("span");
      name.textContent = n.host;
      const info = document.createElement("span");
      info.className = n.reachable ? "up" : "down";
      info.textContent = n.reachable
        ? "up" + (n.gpu_mem_used_gb != null ? " · " + n.gpu_mem_used_gb + " GiB" : "")
        : "unreachable";
      li.append(name, info);
      return li;
    })
  );

  // Recipe catalog: active badge + per-recipe Start (confirm-gated server-side).
  const busy = vm.state === "starting" || vm.state === "stopping";
  $("recipes").replaceChildren(
    ...(vm.recipes || []).map((r) => {
      const li = document.createElement("li");
      const name = document.createElement("span");
      name.className = "recipe-name";
      const sub = document.createElement("span");
      sub.className = "recipe-sub";
      sub.textContent = r.nodes.join(" + ") + " · :" + r.port;
      name.textContent = r.served_model + " ";
      name.append(sub);
      if (r.is_active) {
        const b = document.createElement("span");
        b.className = "badge active";
        b.textContent = "RUNNING";
        name.append(b);
      } else if (r.locked) {
        const b = document.createElement("span");
        b.className = "badge locked";
        b.textContent = "needs swap";
        name.append(b);
      }
      const btn = document.createElement("button");
      btn.className = "start-btn";
      btn.textContent = r.recipe_id === pendingAction ? "Starting…" : "Start";
      btn.disabled = r.is_active || busy || pendingAction !== null;
      btn.addEventListener("click", () => doStart(r.recipe_id, false));
      li.append(name, btn);
      return li;
    })
  );

  const btn = $("main-button");
  btn.textContent = vm.button.label;
  btn.disabled = !vm.button.enabled || pendingAction !== null;
  btn.className = vm.button.action === "stop" ? "primary-btn stop" : "primary-btn";

  // Monitor tile: configurable URLs (empty = hidden).
  const monitors = vm.monitors || {};
  const frame = $("sparkdash-frame");
  const glink = $("grafana-link");
  if (frame && monitors.sparkdash) {
    if (frame.getAttribute("src") !== monitors.sparkdash) frame.setAttribute("src", monitors.sparkdash);
    frame.style.display = "";
  } else if (frame) {
    frame.style.display = "none";
  }
  if (glink) {
    if (monitors.grafana) {
      glink.href = monitors.grafana;
      glink.style.display = "";
    } else {
      glink.style.display = "none";
    }
  }
}

async function refresh() {
  try {
    const params = jobId ? "?job_id=" + encodeURIComponent(jobId) : "";
    const vm = await api("/api/ui" + params);
    if (!vm.job || vm.job.state === "done" || vm.job.state === "failed") jobId = null;
    render(vm);
    $("offline").classList.add("hidden");
  } catch (e) {
    if (e.code !== "unauthorized") $("offline").classList.remove("hidden");
  }
}

function showBanner(text) {
  const banner = $("banner");
  banner.textContent = text;
  banner.classList.remove("hidden");
}

/* ---------- jobs page ---------- */

async function loadJobs() {
  try {
    const d = await api("/api/jobs/recent?limit=20");
    const list = d.jobs || [];
    $("jobs-list").replaceChildren(
      ...(list.length
        ? list.map((j) => {
            const li = document.createElement("li");
            li.className = "job-item";
            const head = document.createElement("div");
            head.className = "job-head";
            const left = document.createElement("span");
            left.textContent =
              (j.action || "?") + " · " + new Date((j.created_at || 0) * 1000).toLocaleString();
            const st = document.createElement("span");
            st.className = "job-state " + (j.state || "");
            st.textContent = j.state || "?";
            head.append(left, st);
            li.append(head);
            if (j.result) {
              const r = document.createElement("div");
              r.className = "job-result";
              r.textContent = j.result;
              li.append(r);
            }
            if (j.output_tail && j.output_tail.length) {
              const det = document.createElement("details");
              const sum = document.createElement("summary");
              sum.textContent = "output tail (" + j.output_tail.length + " lines)";
              const pre = document.createElement("pre");
              pre.textContent = j.output_tail.join("\n");
              det.append(sum, pre);
              li.append(det);
            }
            return li;
          })
        : [Object.assign(document.createElement("li"), { className: "muted", textContent: "No jobs yet." })])
    );
  } catch (e) {
    if (e.code !== "unauthorized") {
      const li = document.createElement("li");
      li.className = "muted";
      li.textContent = "Could not load jobs — " + (e.message || "offline");
      $("jobs-list").replaceChildren(li);
    }
  }
}

/* ---------- actions ---------- */

function bindButton() {
  const btn = $("main-button");
  let pressTimer = null;
  let longFired = false;

  btn.addEventListener("pointerdown", () => {
    if (btn.disabled) return;
    longFired = false;
    if (btn.classList.contains("stop")) {
      btn.classList.add("pressing");
      pressTimer = setTimeout(() => {
        longFired = true;
        btn.classList.remove("pressing");
        doStop();
      }, LONGPRESS_MS);
    }
  });

  const cancel = () => {
    if (pressTimer) clearTimeout(pressTimer);
    pressTimer = null;
    btn.classList.remove("pressing");
    // Short tap on the stop button does nothing (FR-2 guard).
    if (!longFired && !btn.classList.contains("stop") && !btn.disabled) doStart(recipe(), false);
  };
  btn.addEventListener("pointerup", cancel);
  btn.addEventListener("pointercancel", cancel);
  btn.addEventListener("pointerleave", cancel);
  btn.addEventListener("contextmenu", (e) => e.preventDefault());
}

async function doStart(recipeId, confirmSwap) {
  if (pendingAction) return;
  pendingAction = recipeId;
  try {
    const r = await api("/api/server/" + recipeId + "/start", {
      method: "POST",
      body: JSON.stringify({ confirm_swap: !!confirmSwap }),
    });
    if (r.job_id) jobId = r.job_id;
  } catch (e) {
    if (e.code === "port-held-needs-confirm") {
      pendingAction = null;
      if (window.confirm(e.message + " — proceed?")) return doStart(recipeId, true);
    } else if (e.code === "operation-in-progress") {
      showBanner(e.message);
    } else if (e.code !== "unauthorized") {
      window.alert("Start failed: " + e.message);
    }
  } finally {
    if (pendingAction) pendingAction = null;
    refresh();
  }
}

async function doStop() {
  const target = lastVm && lastVm.active_recipe;
  if (!target) {
    window.alert("Nothing is running.");
    return;
  }
  if (pendingAction) return;
  pendingAction = target;
  try {
    const r = await api("/api/server/" + target + "/stop", { method: "POST" });
    if (r.job_id) jobId = r.job_id;
  } catch (e) {
    if (e.code === "operation-in-progress") {
      showBanner(e.message);
    } else if (e.code !== "unauthorized") {
      window.alert("Stop failed: " + e.message);
    }
  } finally {
    if (pendingAction) pendingAction = null;
    refresh();
  }
}

/* ---------- boot ---------- */

initRouting();
bindButton();
refresh();
timer = setInterval(refresh, REFRESH_MS);
