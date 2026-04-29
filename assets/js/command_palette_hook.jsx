import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

const MAX_RESULTS = 50;
const SOURCES_URL = "/command-palette/sources";

function shouldIgnoreTrigger(e) {
  const t = e.target;
  if (!t || !t.closest) return false;
  return !!t.closest('.modal.show, .monaco-editor, [contenteditable="true"]');
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
  window.location.href = source.path;
}

function ResultRow({ source, isActive, activeRef, onActivate, onSelect }) {
  return (
    <li
      ref={isActive ? activeRef : null}
      className={"lf-cmdk-item" + (isActive ? " lf-cmdk-active" : "")}
      onMouseEnter={onActivate}
      onClick={onSelect}
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
  );
}

function CommandPalette({ onClose }) {
  const [sources, setSources] = useState([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef(null);
  const activeRef = useRef(null);
  const scrollOnNextRender = useRef(false);

  useEffect(() => {
    let cancelled = false;
    fetch(SOURCES_URL, { credentials: "same-origin", headers: { Accept: "application/json" } })
      .then((r) => (r.ok ? r.json() : { sources: [] }))
      .catch(() => ({ sources: [] }))
      .then((reply) => {
        if (cancelled) return;
        setSources((reply && reply.sources) || []);
        setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    inputRef.current && inputRef.current.focus();
  }, []);

  const results = useMemo(() => rankSources(sources, query), [sources, query]);

  useEffect(() => {
    if (activeIndex > 0 && activeIndex >= results.length) setActiveIndex(0);
  }, [results, activeIndex]);

  useEffect(() => {
    if (!scrollOnNextRender.current) return;
    scrollOnNextRender.current = false;
    activeRef.current && activeRef.current.scrollIntoView({ block: "nearest" });
  }, [activeIndex]);

  function moveActive(delta) {
    if (results.length === 0) return;
    scrollOnNextRender.current = true;
    setActiveIndex((i) => (i + delta + results.length) % results.length);
  }

  function onKeyDown(e) {
    if (e.key === "Escape") {
      e.preventDefault();
      onClose();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      moveActive(1);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      moveActive(-1);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const source = results[activeIndex];
      if (source) navigateTo(source);
    }
  }

  function renderBody() {
    if (loading) return <li className="lf-cmdk-empty">Loading…</li>;
    if (results.length === 0) {
      return (
        <li className="lf-cmdk-empty">
          {sources.length === 0 ? "No sources" : "No matches"}
        </li>
      );
    }
    return results.map((source, idx) => (
      <ResultRow
        key={source.id}
        source={source}
        isActive={idx === activeIndex}
        activeRef={activeRef}
        onActivate={() => {
          if (idx !== activeIndex) setActiveIndex(idx);
        }}
        onSelect={() => navigateTo(source)}
      />
    ));
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
        <ul className="lf-cmdk-list">{renderBody()}</ul>
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
