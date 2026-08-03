import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";

const MAX_RESULTS = 50;
const SOURCES_URL = "/command-palette/sources";

function shouldIgnoreTrigger(e) {
  const t = e.target;
  if (!t || !t.closest) return false;
  return !!t.closest('.modal.show, .monaco-editor, [contenteditable="true"]');
}

function timezoneSelectEl() {
  return document.querySelector('select[name="search_timezone"]');
}

function applyTimezone(value) {
  const select = timezoneSelectEl();
  if (!select) return;
  select.value = value;
  select.dispatchEvent(new Event("input", { bubbles: true }));
  select.dispatchEvent(new Event("change", { bubbles: true }));
}

export function filterByText(items, query, getText) {
  const q = query.trim().toLowerCase();
  if (!q) return items;
  return items.filter((item) => {
    const text = getText(item);
    return text && text.toLowerCase().includes(q);
  });
}

const tzMetaCache = new Map();

function readPart(date, tz, type) {
  try {
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      timeZoneName: type,
    }).formatToParts(date);
    const part = parts.find((p) => p.type === "timeZoneName");
    return part ? part.value : "";
  } catch (e) {
    return "";
  }
}

export function getZoneMeta(tz) {
  if (tzMetaCache.has(tz)) return tzMetaCache.get(tz);

  const now = new Date();

  const m = readPart(now, tz, "shortOffset").match(/([+-])(\d{1,2})(?::?(\d{2}))?/);
  const offsetLabel = m
    ? `UTC${m[1]}${m[2].padStart(2, "0")}:${(m[3] || "00").padStart(2, "0")}`
    : "UTC+00:00";

  let abbr = readPart(now, tz, "short");
  if (!abbr || /\d/.test(abbr) || /^(GMT|UTC)$/.test(abbr)) abbr = "";

  const meta = { offsetLabel, abbr };
  tzMetaCache.set(tz, meta);
  return meta;
}

export function buildTimezoneData() {
  const select = timezoneSelectEl();
  const values = select ? Array.from(select.options).map((o) => o.value) : [];
  const valueSet = new Set(values);

  const items = values.map((value) => {
    const meta = getZoneMeta(value);
    const slash = value.indexOf("/");
    const region = slash === -1 ? "General" : value.slice(0, slash);
    const city = (slash === -1 ? value : value.slice(slash + 1)).replace(/_/g, " ");
    return {
      key: `tz:${value}`,
      value,
      region,
      primary: city,
      secondary: meta.abbr,
      offsetLabel: meta.offsetLabel,
      searchText: `${value} ${city} ${region} ${meta.abbr}`.toLowerCase(),
    };
  });

  const browserTz = Intl.DateTimeFormat().resolvedOptions().timeZone;
  const pinnedDefs = [
    { value: browserTz, primary: "Browser Time", secondary: browserTz },
    { value: "Etc/UTC", primary: "Coordinated Universal Time", secondary: "" },
  ];

  const pinned = pinnedDefs
    .filter((p) => p.value && valueSet.has(p.value))
    .map((p) => ({
      key: `tz-pinned:${p.value}`,
      value: p.value,
      primary: p.primary,
      secondary: p.secondary,
      offsetLabel: getZoneMeta(p.value).offsetLabel,
      searchText: `${p.primary} ${p.secondary} ${p.value}`.toLowerCase(),
    }));

  const pinnedValues = new Set(pinned.map((p) => p.value));

  return { items: items.filter((i) => !pinnedValues.has(i.value)), pinned };
}

export function rankSources(sources, query) {
  const matches = filterByText(sources, query, (s) => s.name).slice();
  matches.sort((a, b) => {
    if (a.favorite !== b.favorite) return a.favorite ? -1 : 1;
    return (a.name || "").localeCompare(b.name || "");
  });
  return matches.slice(0, MAX_RESULTS);
}

function navigateTo(source) {
  window.location.href = source.path;
}

function SourceRow({ source }) {
  return (
    <>
      {source.favorite && <span className="lf-cmdk-favorite">★</span>}
      <span className="lf-cmdk-name">{source.name}</span>
      {source.service_name && <span className="lf-cmdk-service">{source.service_name}</span>}
      {source.team && source.team.name && (
        <span className="lf-cmdk-team">{source.team.name}</span>
      )}
    </>
  );
}

function CommandRow({ command }) {
  return (
    <>
      {command.icon && <span className="lf-cmdk-command-icon">{command.icon}</span>}
      <span className="lf-cmdk-name">{command.name}</span>
      <span className="lf-cmdk-hint">{command.hint || "→"}</span>
    </>
  );
}

function TimezoneRow({ item }) {
  return (
    <>
      <span className="lf-cmdk-tz-main">
        <span className="lf-cmdk-tz-name">{item.primary}</span>
        {item.secondary && <span className="lf-cmdk-secondary">{item.secondary}</span>}
      </span>
      <span className="lf-cmdk-offset">{item.offsetLabel}</span>
    </>
  );
}

function ResultRow({ entry, isActive, activeRef, onActivate, onSelect }) {
  return (
    <li
      ref={isActive ? activeRef : null}
      className={"lf-cmdk-item" + (isActive ? " lf-cmdk-active" : "")}
      onMouseEnter={onActivate}
      onClick={onSelect}
    >
      {entry.render()}
    </li>
  );
}

function CommandPalette({ onClose }) {
  const [sources, setSources] = useState([]);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("root");
  const [query, setQuery] = useState("");
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef(null);
  const activeRef = useRef(null);
  const scrollOnNextRender = useRef(false);
  const stateRef = useRef({});

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

  function goToView(nextView) {
    setView(nextView);
    setQuery("");
    setActiveIndex(0);
  }

  const commands = useMemo(() => {
    const list = [];
    if (timezoneSelectEl()) {
      list.push({
        id: "set-timezone",
        type: "command",
        name: "Set display timezone…",
        hint: "→",
        icon: "🕐",
        keywords: "timezone tz time zone utc",
        run: () => goToView("timezone"),
      });
    }
    return list;
  }, []);

  const tzData = useMemo(() => (view === "timezone" ? buildTimezoneData() : null), [view]);

  const groups = useMemo(() => {
    if (view === "timezone" && tzData) {
      const toEntry = (item) => ({
        key: item.key,
        render: () => <TimezoneRow item={item} />,
        onSelect: () => {
          applyTimezone(item.value);
          onClose();
        },
      });

      const result = [];

      const pinned = filterByText(tzData.pinned, query, (i) => i.searchText);
      if (pinned.length) result.push({ key: "pinned", header: null, entries: pinned.map(toEntry) });

      const matched = filterByText(tzData.items, query, (i) => i.searchText);
      const byRegion = new Map();
      for (const item of matched) {
        if (!byRegion.has(item.region)) byRegion.set(item.region, []);
        byRegion.get(item.region).push(item);
      }

      Array.from(byRegion.keys())
        .sort((a, b) => a.localeCompare(b))
        .forEach((region) => {
          const items = byRegion
            .get(region)
            .sort((a, b) => a.primary.localeCompare(b.primary));
          result.push({ key: `region:${region}`, header: region, entries: items.map(toEntry) });
        });

      return result;
    }

    const commandEntries = filterByText(
      commands,
      query,
      (c) => `${c.name} ${c.keywords || ""}`,
    ).map((command) => ({
      key: `cmd:${command.id}`,
      render: () => <CommandRow command={command} />,
      onSelect: () => command.run(),
    }));

    const sourceEntries = rankSources(sources, query).map((source) => ({
      key: `src:${source.id}`,
      render: () => <SourceRow source={source} />,
      onSelect: () => navigateTo(source),
    }));

    return [{ key: "root", header: null, entries: [...commandEntries, ...sourceEntries] }];
  }, [view, tzData, query, commands, sources, onClose]);

  const flatEntries = useMemo(() => groups.flatMap((g) => g.entries), [groups]);

  useEffect(() => {
    if (activeIndex > 0 && activeIndex >= flatEntries.length) setActiveIndex(0);
  }, [flatEntries, activeIndex]);

  useEffect(() => {
    if (!scrollOnNextRender.current) return;
    scrollOnNextRender.current = false;
    activeRef.current && activeRef.current.scrollIntoView({ block: "nearest" });
  }, [activeIndex]);

  stateRef.current = { view, query, flatEntries, activeIndex };

  function moveActive(delta) {
    const { flatEntries } = stateRef.current;
    if (flatEntries.length === 0) return;
    scrollOnNextRender.current = true;
    setActiveIndex((i) => (i + delta + flatEntries.length) % flatEntries.length);
  }

  useEffect(() => {
    function handle(e) {
      if (e.metaKey || e.ctrlKey || e.altKey) return;
      const { view, query, flatEntries, activeIndex } = stateRef.current;

      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        if (view !== "root") goToView("root");
        else onClose();
      } else if (e.key === "ArrowDown") {
        e.preventDefault();
        e.stopPropagation();
        moveActive(1);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        e.stopPropagation();
        moveActive(-1);
      } else if (e.key === "Enter") {
        e.preventDefault();
        e.stopPropagation();
        const entry = flatEntries[activeIndex];
        if (entry) entry.onSelect();
      } else if (e.key === "Backspace" && query === "" && view !== "root") {
        e.preventDefault();
        e.stopPropagation();
        goToView("root");
      }
    }

    document.addEventListener("keydown", handle, true);
    return () => document.removeEventListener("keydown", handle, true);
  }, []);

  function placeholder() {
    return view === "timezone"
      ? "Type to search (city, region, abbreviation)"
      : "Type a command or jump to a source…";
  }

  function renderBody() {
    if (view === "root" && loading) {
      return <li className="lf-cmdk-empty">Loading…</li>;
    }
    if (flatEntries.length === 0) {
      const emptyText =
        view === "timezone"
          ? "No matching timezone"
          : query
            ? "No matches"
            : "No commands or sources";
      return <li className="lf-cmdk-empty">{emptyText}</li>;
    }

    let running = 0;
    return groups.map((group) => (
      <React.Fragment key={group.key}>
        {group.header && <li className="lf-cmdk-group-header">{group.header}</li>}
        {group.entries.map((entry) => {
          const idx = running++;
          return (
            <ResultRow
              key={entry.key}
              entry={entry}
              isActive={idx === activeIndex}
              activeRef={activeRef}
              onActivate={() => {
                if (idx !== activeIndex) setActiveIndex(idx);
              }}
              onSelect={entry.onSelect}
            />
          );
        })}
      </React.Fragment>
    ));
  }

  return (
    <div className="lf-cmdk-overlay" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className="lf-cmdk-card">
        {view !== "root" && (
          <div className="lf-cmdk-breadcrumb">
            <button type="button" className="lf-cmdk-back" onClick={() => goToView("root")}>
              ← back
            </button>
            <span className="lf-cmdk-breadcrumb-label">Display timezone</span>
          </div>
        )}
        <input
          ref={inputRef}
          type="text"
          className="lf-cmdk-input"
          placeholder={placeholder()}
          autoComplete="off"
          spellCheck={false}
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setActiveIndex(0);
          }}
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

if (typeof document !== "undefined") {
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
}
