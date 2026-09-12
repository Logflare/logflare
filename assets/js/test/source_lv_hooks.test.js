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

const mountSearchList = (tailing) => {
  const hook = Object.create(hooks.SourceLogsSearchList);
  const handlers = {};

  hook.el = { dataset: { tailing } };
  hook.handleEvent = vi.fn((name, callback) => {
    handlers[name] = callback;
  });

  hook.mounted();

  return { hook, handlers };
};

describe("SourceLogsSearchList", () => {
  beforeEach(() => {
    scrollToPageBottom.mockClear();
    vi.stubGlobal("requestAnimationFrame", (callback) => callback());
  });

  it("scrolls to the bottom when the LiveView pushes scroll-to-bottom", () => {
    const { handlers } = mountSearchList("false");

    expect(scrollToPageBottom).not.toHaveBeenCalled();

    handlers["scroll-to-bottom"]();

    expect(scrollToPageBottom).toHaveBeenCalledTimes(1);
  });

  it("scrolls to the bottom on mount only while tailing", () => {
    mountSearchList("true");
    expect(scrollToPageBottom).toHaveBeenCalledTimes(1);

    scrollToPageBottom.mockClear();

    mountSearchList("false");
    expect(scrollToPageBottom).not.toHaveBeenCalled();
  });
});
