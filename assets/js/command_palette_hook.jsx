// CMD/CTRL+K source quick-switcher.
// Self-contained: a top-level keydown listener mounts a React overlay on first
// open, fetches sources from /command-palette/sources, and unmounts on close.
// Each source carries its team; navigation uses ?t=<team.id>.

import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

const MAX_RESULTS = 50;
const SOURCES_URL = "/command-palette/sources";

function shouldIgnoreTrigger(e) {
  const t = e.target;
  if (document.querySelector(".modal.show")) return true;
  if (!t || !t.closest) return false;
  return !!t.closest('.monaco-editor, [contenteditable="true"]');
}

function rankSources(sources, query) {
  const q = query.trim().toLowerCase();
  const matches = q
    ? sources.filter((s) => s.name && s.name.toLowerCase().includes(q))
    : sources.slice();
  matches.sort((a, b) => {
    if (a.favorite !== b.favorite) return a.favorite ? -1 : 1;
    return (a.name || "").localeCompare(b.name || "");
  });
  return matches.slice(0, MAX_RESULTS);
}

function navigateTo(source) {
  const path = "/sources/" + source.id;
  const t = source.team && source.team.id;
  window.location.href = t ? path + "?t=" + encodeURIComponent(t) : path;
}

function CommandPalette({ onClose }) {
  const [sources, setSources] = useState(null);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef(null);
  const activeRef = useRef(null);

  useEffect(() => {
    let cancelled = false;
    fetch(SOURCES_URL, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then((r) => (r.ok ? r.json() : { sources: [] }))
      .catch(() => ({ sources: [] }))
      .then((reply) => {
        if (cancelled) return;
        setSources((reply && reply.sources) || []);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    inputRef.current && inputRef.current.focus();
  }, []);

  const results = useMemo(
    () => (sources ? rankSources(sources, query) : []),
    [sources, query],
  );

  const clampedIndex = results.length === 0
    ? 0
    : Math.min(Math.max(activeIndex, 0), results.length - 1);

  useEffect(() => {
    activeRef.current && activeRef.current.scrollIntoView({ block: "nearest" });
  }, [clampedIndex]);

  function onKeyDown(e) {
    if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      if (results.length > 0) setActiveIndex((clampedIndex + 1) % results.length);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      if (results.length > 0) setActiveIndex((clampedIndex - 1 + results.length) % results.length);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const source = results[clampedIndex];
      if (source) navigateTo(source);
    }
  }

  return (
    <div className="lf-cmdk-overlay" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="lf-cmdk-card">
        <input
          ref={inputRef}
          type="text"
          className="lf-cmdk-input"
          placeholder="Jump to source…"
          autoComplete="off"
          spellCheck={false}
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setActiveIndex(0);
          }}
          onKeyDown={onKeyDown}
        />
        <ul className="lf-cmdk-list">
          {sources === null ? (
            <li className="lf-cmdk-empty">Loading…</li>
          ) : results.length === 0 ? (
            <li className="lf-cmdk-empty">
              {sources.length === 0 ? "No sources" : "No matches"}
            </li>
          ) : (
            results.map((source, idx) => (
              <li
                key={source.id}
                ref={idx === clampedIndex ? activeRef : null}
                className={"lf-cmdk-item" + (idx === clampedIndex ? " lf-cmdk-active" : "")}
                onMouseEnter={() => setActiveIndex(idx)}
                onClick={() => navigateTo(source)}
              >
                {source.favorite && <span className="lf-cmdk-favorite">★</span>}
                <span className="lf-cmdk-name">{source.name}</span>
                {source.service_name && (
                  <span className="lf-cmdk-service">{source.service_name}</span>
                )}
                {source.team && source.team.name && (
                  <span className="lf-cmdk-team">{source.team.name}</span>
                )}
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
}

let mount = null;

function open() {
  if (mount) return;
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);
  mount = { container, root };
  root.render(<CommandPalette onClose={close} />);
}

function close() {
  if (!mount) return;
  mount.root.unmount();
  mount.container.remove();
  mount = null;
}

document.addEventListener(
  "keydown",
  (e) => {
    const isToggle =
      (e.metaKey || e.ctrlKey) && !e.shiftKey && !e.altKey && e.key && e.key.toLowerCase() === "k";
    if (!isToggle) return;
    if (mount) {
      e.preventDefault();
      close();
      return;
    }
    if (shouldIgnoreTrigger(e)) return;
    e.preventDefault();
    open();
  },
  true,
);
