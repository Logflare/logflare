// CMD/CTRL+K source quick-switcher.
// Self-contained: bound at module load, fetches sources on first open from
// /command-palette/sources. Each source carries its team; navigation uses ?t=<team.id>.

const MAX_RESULTS = 50;
const SOURCES_URL = "/command-palette/sources";

const state = {
  sources: null,
  fetching: false,
  query: "",
  activeIndex: 0,
  overlay: null,
  input: null,
  list: null,
};

function isOpen() {
  return state.overlay !== null;
}

function shouldIgnoreTrigger(e) {
  const t = e.target;
  if (document.querySelector(".modal.show")) return true;
  if (!t || !t.closest) return false;
  return !!t.closest('.monaco-editor, [contenteditable="true"]');
}

function filterSources() {
  if (!state.sources) return [];
  const q = state.query.trim().toLowerCase();
  const matches = q
    ? state.sources.filter((s) => s.name && s.name.toLowerCase().includes(q))
    : state.sources.slice();
  matches.sort((a, b) => {
    if (a.favorite !== b.favorite) return a.favorite ? -1 : 1;
    return (a.name || "").localeCompare(b.name || "");
  });
  return matches.slice(0, MAX_RESULTS);
}

function renderResults() {
  if (!state.list) return;
  state.list.innerHTML = "";

  if (state.sources === null) {
    const li = document.createElement("li");
    li.className = "lf-cmdk-empty";
    li.textContent = "Loading…";
    state.list.appendChild(li);
    return;
  }

  const results = filterSources();

  if (results.length === 0) {
    const li = document.createElement("li");
    li.className = "lf-cmdk-empty";
    li.textContent = state.sources.length === 0 ? "No sources" : "No matches";
    state.list.appendChild(li);
    return;
  }

  if (state.activeIndex >= results.length) state.activeIndex = results.length - 1;
  if (state.activeIndex < 0) state.activeIndex = 0;

  results.forEach((source, idx) => {
    const li = document.createElement("li");
    li.className = "lf-cmdk-item" + (idx === state.activeIndex ? " lf-cmdk-active" : "");

    if (source.favorite) {
      const star = document.createElement("span");
      star.textContent = "★";
      star.className = "lf-cmdk-favorite";
      li.appendChild(star);
    }

    const name = document.createElement("span");
    name.className = "lf-cmdk-name";
    name.textContent = source.name;
    li.appendChild(name);

    if (source.service_name) {
      const svc = document.createElement("span");
      svc.className = "lf-cmdk-service";
      svc.textContent = source.service_name;
      li.appendChild(svc);
    }

    if (source.team && source.team.name) {
      const team = document.createElement("span");
      team.className = "lf-cmdk-team";
      team.textContent = source.team.name;
      li.appendChild(team);
    }

    li.addEventListener("mouseenter", () => {
      if (state.activeIndex !== idx) {
        state.activeIndex = idx;
        renderResults();
      }
    });
    li.addEventListener("click", () => navigateTo(source));
    state.list.appendChild(li);
  });
}

function renderOverlay() {
  if (state.overlay) return;

  const overlay = document.createElement("div");
  overlay.className = "lf-cmdk-overlay";
  overlay.addEventListener("click", (e) => {
    if (e.target === overlay) closePalette();
  });

  const card = document.createElement("div");
  card.className = "lf-cmdk-card";
  overlay.appendChild(card);

  const input = document.createElement("input");
  input.type = "text";
  input.placeholder = "Jump to source…";
  input.className = "lf-cmdk-input";
  input.autocomplete = "off";
  input.spellcheck = false;
  input.addEventListener("input", (e) => {
    state.query = e.target.value;
    state.activeIndex = 0;
    renderResults();
  });
  card.appendChild(input);

  const list = document.createElement("ul");
  list.className = "lf-cmdk-list";
  card.appendChild(list);

  document.body.appendChild(overlay);
  state.overlay = overlay;
  state.input = input;
  state.list = list;
  renderResults();
}

function openPalette() {
  state.query = "";
  state.activeIndex = 0;
  renderOverlay();
  maybeFetchSources();
  setTimeout(() => state.input && state.input.focus(), 0);
}

function maybeFetchSources() {
  if (state.sources !== null || state.fetching) return;
  state.fetching = true;
  fetch(SOURCES_URL, { credentials: "same-origin", headers: { Accept: "application/json" } })
    .then((r) => (r.ok ? r.json() : { sources: [] }))
    .catch(() => ({ sources: [] }))
    .then((reply) => {
      state.fetching = false;
      state.sources = (reply && reply.sources) || [];
      if (isOpen()) renderResults();
    });
}

function closePalette() {
  if (state.overlay) {
    state.overlay.remove();
    state.overlay = null;
    state.input = null;
    state.list = null;
  }
}

function moveActive(delta) {
  const results = filterSources();
  if (results.length === 0) return;
  state.activeIndex = (state.activeIndex + delta + results.length) % results.length;
  renderResults();
  const activeEl = state.list && state.list.children[state.activeIndex];
  if (activeEl && activeEl.scrollIntoView) {
    activeEl.scrollIntoView({ block: "nearest" });
  }
}

function selectActive() {
  const results = filterSources();
  const source = results[state.activeIndex];
  if (source) navigateTo(source);
}

function navigateTo(source) {
  const path = "/sources/" + source.id;
  const t = source.team && source.team.id;
  window.location.href = t ? path + "?t=" + encodeURIComponent(t) : path;
}

function onKeydown(e) {
  const isToggle =
    (e.metaKey || e.ctrlKey) && !e.shiftKey && !e.altKey && e.key && e.key.toLowerCase() === "k";

  if (isToggle) {
    if (isOpen()) {
      e.preventDefault();
      closePalette();
      return;
    }
    if (shouldIgnoreTrigger(e)) return;
    e.preventDefault();
    openPalette();
    return;
  }

  if (!isOpen()) return;

  if (e.key === "Escape") {
    e.preventDefault();
    closePalette();
  } else if (e.key === "ArrowDown") {
    e.preventDefault();
    moveActive(1);
  } else if (e.key === "ArrowUp") {
    e.preventDefault();
    moveActive(-1);
  } else if (e.key === "Enter") {
    e.preventDefault();
    selectActive();
  }
}

document.addEventListener("keydown", onKeydown, true);
