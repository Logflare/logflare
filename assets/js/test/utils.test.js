// @vitest-environment jsdom
import { describe, expect, it, vi } from "vitest";

vi.mock("clipboard", () => ({ default: vi.fn() }));
vi.mock("jquery", () => ({ default: vi.fn(() => ({ tooltip: vi.fn() })) }));

const { scrollToPageBottom } = await import("../utils.js");

describe("scrollToPageBottom", () => {
  it("scrolls the window to the full document height", () => {
    const scrollTo = vi.fn();
    vi.stubGlobal("scrollTo", scrollTo);
    Object.defineProperty(document.body, "scrollHeight", {
      configurable: true,
      value: 4321,
    });

    scrollToPageBottom();

    expect(scrollTo).toHaveBeenCalledWith(0, 4321);
  });
});
