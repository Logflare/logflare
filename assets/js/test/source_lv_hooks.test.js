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
