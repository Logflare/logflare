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

  it("scrolls the given event into view when the LiveView pushes scroll-to-event", () => {
    const { handlers } = mountSearchList();
    const scrollIntoView = vi.fn();
    document.body.innerHTML = '<ul id="logs-list"><li id="log-events-a-1"></li></ul>';
    document.getElementById("log-events-a-1").scrollIntoView = scrollIntoView;

    handlers["scroll-to-event"]({ id: "log-events-a-1" });

    expect(scrollIntoView).toHaveBeenCalledWith({ block: "start" });
    expect(scrollToPageBottom).not.toHaveBeenCalled();
  });

  it("ignores scroll-to-event for an element that is not in the DOM", () => {
    const { handlers } = mountSearchList();
    document.body.innerHTML = "";

    expect(() => handlers["scroll-to-event"]({ id: "missing" })).not.toThrow();
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
