// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../utils", () => ({
  activateClipboardForSelector: vi.fn(),
  activateDelegatedTooltips: vi.fn(),
  scrollToPageBottom: vi.fn(),
}));
vi.mock("../logs", () => ({ applyToAllLogTimestamps: vi.fn() }));
vi.mock("../formatters", () => ({ timestampNsToAgo: vi.fn() }));
vi.mock("../vendor/idle", () => ({ default: vi.fn() }));

const { scrollToPageBottom } = await import("../utils");
const { default: hooks } = await import("../source_lv_hooks.js");

describe("SavedSearchShortcuts", () => {
  it("reserves room for Show more, expands inline, and reflows when resized or updated", () => {
    let resize;
    vi.stubGlobal("ResizeObserver", class {
      constructor(callback) { resize = callback; }
      observe() {}
      disconnect() {}
    });
    document.body.innerHTML = `
      <div id="shortcuts">
        <a href="#errors">Errors</a><a href="#latency">Slow API requests</a>
        <a href="#sign-ins">Failed sign-ins</a><a href="#timeouts">Timeouts</a>
        <a href="#limits">Rate limits</a>
        <button type="button" data-expand-shortcuts hidden>Show more</button>
      </div>`;
    const el = document.getElementById("shortcuts");
    const shortcuts = Array.from(el.querySelectorAll("a"));
    const toggle = el.querySelector("button");
    let tops = [0, 0, 36, 36, 72];
    let toggleTops = [0, 0, 36, 36, 72, 72];
    shortcuts.forEach((shortcut, index) => {
      Object.defineProperty(shortcut, "offsetTop", { get: () => tops[index] });
    });
    Object.defineProperty(toggle, "offsetTop", {
      get: () => toggleTops[shortcuts.filter((shortcut) => !shortcut.hidden).length],
    });
    const visibleNames = () => shortcuts.filter((shortcut) => !shortcut.hidden).map((shortcut) => shortcut.textContent);
    const hook = Object.assign(Object.create(hooks.SavedSearchShortcuts), { el });
    hook.mounted();

    expect(visibleNames()).toEqual(["Errors", "Slow API requests", "Failed sign-ins"]);
    expect(toggle.hidden).toBe(false);
    expect(toggle.getAttribute("aria-expanded")).toBe("false");

    toggle.click();
    expect(visibleNames()).toHaveLength(5);
    expect(toggle.textContent).toBe("Show less");
    expect(toggle.getAttribute("aria-expanded")).toBe("true");

    tops = [0, 36, 72, 108, 144];
    toggleTops = [0, 36, 72, 108, 144, 180];
    resize();
    expect(visibleNames()).toHaveLength(5);
    toggle.click();
    expect(visibleNames()).toEqual(["Errors"]);

    tops = [0, 0, 0, 36, 36];
    resize();
    expect(visibleNames()).toHaveLength(5);
    expect(toggle.hidden).toBe(true);

    tops = [0, 0, 36, 36, 72];
    toggleTops = [0, 0, 36, 36, 72, 72];
    hook.updated();
    expect(visibleNames()).toEqual(["Errors", "Slow API requests", "Failed sign-ins"]);
    expect(toggle.textContent).toBe("Show more");
    expect(toggle.hidden).toBe(false);
    hook.destroyed();
    vi.unstubAllGlobals();
  });
});

const mountSearchList = () => {
  const hook = Object.create(hooks.SourceLogsSearchList);
  const handlers = {};

  hook.el = {
    querySelectorAll: () => [],
  };
  hook.handleEvent = vi.fn((name, callback) => {
    handlers[name] = callback;
  });
  hook.pushEvent = vi.fn();
  hook.restoreScrollAnchor = vi.fn();

  hook.mounted();

  return { hook, handlers };
};

describe("SourceLogsSearchList", () => {
  beforeEach(() => {
    scrollToPageBottom.mockClear();
    vi.stubGlobal("requestAnimationFrame", (callback) => callback());
  });

  it("scrolls to the bottom when the LiveView pushes scroll-to-bottom", () => {
    const { handlers } = mountSearchList();

    expect(scrollToPageBottom).not.toHaveBeenCalled();

    handlers["scroll-to-bottom"]();

    expect(scrollToPageBottom).toHaveBeenCalledTimes(1);
  });

  it("keeps the pending scroll instead of restoring the anchor on a later update", () => {
    const { hook, handlers } = mountSearchList();
    const rafQueue = [];
    vi.stubGlobal("requestAnimationFrame", (callback) => rafQueue.push(callback));
    vi.stubGlobal("IntersectionObserver", class {
      observe() {}
    });
    document.body.innerHTML = '<div id="observer-target"></div>';

    handlers["scroll-to-bottom"]();

    hook.updated();

    expect(hook.restoreScrollAnchor).not.toHaveBeenCalled();

    rafQueue.forEach((callback) => callback());

    expect(scrollToPageBottom).toHaveBeenCalledTimes(1);
    expect(hook.pendingScrollToBottom).toBe(false);
  });

  it("does not scroll on its own; the LiveView drives every scroll", () => {
    mountSearchList();

    expect(scrollToPageBottom).not.toHaveBeenCalled();
  });
});

describe("SourceLogsSearchList scroll anchor", () => {
  let scrollBy;
  let scrollTo;

  const mountWithLogList = () => {
    document.body.innerHTML = `
      <div id="source-logs-search-list">
        <ul id="logs-list">
          <li id="log-events-a-1" data-event-id="a-1"></li>
          <li id="log-events-a-2" data-event-id="a-2"></li>
        </ul>
      </div>`;

    const hook = Object.create(hooks.SourceLogsSearchList);
    hook.el = document.getElementById("source-logs-search-list");
    hook.handleEvent = vi.fn();
    hook.pushEvent = vi.fn();
    hook.mounted();

    return hook;
  };

  const stubRect = (element, top) => {
    element.getBoundingClientRect = () => ({ top, bottom: top + 20 });
  };

  beforeEach(() => {
    scrollBy = vi.fn();
    scrollTo = vi.fn();
    vi.stubGlobal("scrollBy", scrollBy);
    vi.stubGlobal("scrollTo", scrollTo);
  });

  it("holds the viewport when taller rows load above the visible row", () => {
    const hook = mountWithLogList();
    const anchor = document.getElementById("log-events-a-1");

    stubRect(anchor, 100);
    hook.captureScrollAnchor();

    stubRect(anchor, 420);
    hook.restoreScrollAnchor();

    expect(scrollBy).toHaveBeenCalledWith(0, 320);
  });

  it("does not scroll when the anchor row has not moved", () => {
    const hook = mountWithLogList();
    const anchor = document.getElementById("log-events-a-1");

    stubRect(anchor, 100);
    hook.captureScrollAnchor();
    hook.restoreScrollAnchor();

    expect(scrollBy).not.toHaveBeenCalled();
  });

  it("leaves the scroll alone when the anchor row is gone", () => {
    const hook = mountWithLogList();

    stubRect(document.getElementById("log-events-a-1"), 100);
    hook.captureScrollAnchor();

    document.getElementById("logs-list").innerHTML = "";
    hook.restoreScrollAnchor();

    expect(scrollBy).not.toHaveBeenCalled();
    expect(scrollTo).not.toHaveBeenCalled();
  });

  it("restores the anchor before the next frame so a following diff sees the corrected position", () => {
    const hook = mountWithLogList();
    const anchor = document.getElementById("log-events-a-1");
    const rafQueue = [];
    vi.stubGlobal("requestAnimationFrame", (callback) => rafQueue.push(callback));

    stubRect(anchor, 100);
    hook.captureScrollAnchor();

    stubRect(anchor, 420);
    hook.restoreScrollAnchor();

    expect(scrollBy).toHaveBeenCalledWith(0, 320);
    expect(rafQueue).toHaveLength(0);
  });
});
