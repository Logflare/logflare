import { afterEach, describe, expect, it, vi } from "vitest";

process.env.TZ = "UTC";

const {
  buildTimezoneData,
  filterByText,
  getZoneMeta,
  rankSources,
} = await import("../command_palette_hook.jsx");

function stubSelect(values) {
  vi.stubGlobal("document", {
    querySelector: () => ({ options: values.map((value) => ({ value })) }),
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("filterByText", () => {
  const items = [{ n: "Europe/Berlin" }, { n: "America/New_York" }, { n: "" }];
  const getText = (i) => i.n;

  it("returns all items for an empty or whitespace query", () => {
    expect(filterByText(items, "", getText)).toHaveLength(3);
    expect(filterByText(items, "   ", getText)).toHaveLength(3);
  });

  it("matches case-insensitively on a substring", () => {
    expect(filterByText(items, "BER", getText)).toEqual([{ n: "Europe/Berlin" }]);
  });

  it("excludes items whose text is falsy", () => {
    expect(filterByText(items, "e", getText).every((i) => i.n)).toBe(true);
  });
});

describe("getZoneMeta", () => {
  it("normalizes fixed offsets to UTC±HH:MM", () => {
    expect(getZoneMeta("Etc/UTC").offsetLabel).toBe("UTC+00:00");
    expect(getZoneMeta("Asia/Tokyo").offsetLabel).toBe("UTC+09:00");
    expect(getZoneMeta("Asia/Kolkata").offsetLabel).toBe("UTC+05:30");
    expect(getZoneMeta("Asia/Kathmandu").offsetLabel).toBe("UTC+05:45");
  });

  it("drops abbreviations that are numeric or GMT/UTC", () => {
    expect(getZoneMeta("Etc/UTC").abbr).toBe("");
    expect(getZoneMeta("Asia/Tokyo").abbr).toBe("");
  });

  it("falls back gracefully for an unknown zone", () => {
    expect(getZoneMeta("Not/AZone")).toEqual({ offsetLabel: "UTC+00:00", abbr: "" });
  });
});

describe("buildTimezoneData", () => {
  it("returns empty data when the select is absent", () => {
    vi.stubGlobal("document", { querySelector: () => null });
    expect(buildTimezoneData()).toEqual({ items: [], pinned: [] });
  });

  it("parses region and city from the zone path", () => {
    stubSelect(["America/New_York", "Europe/Berlin", "GMT"]);
    const { items } = buildTimezoneData();

    const ny = items.find((i) => i.value === "America/New_York");
    expect(ny).toMatchObject({ region: "America", primary: "New York" });

    const gmt = items.find((i) => i.value === "GMT");
    expect(gmt).toMatchObject({ region: "General", primary: "GMT" });
  });

  it("pins Etc/UTC and removes it from the regular list", () => {
    stubSelect(["Etc/UTC", "Europe/Berlin"]);
    const { items, pinned } = buildTimezoneData();

    expect(pinned).toContainEqual(
      expect.objectContaining({ value: "Etc/UTC", primary: "Coordinated Universal Time" }),
    );
    expect(items.some((i) => i.value === "Etc/UTC")).toBe(false);
  });
});

describe("rankSources", () => {
  it("orders favorites first, then alphabetically", () => {
    const sources = [
      { id: 1, name: "zeta" },
      { id: 2, name: "alpha", favorite: true },
      { id: 3, name: "beta" },
      { id: 4, name: "Gamma", favorite: true },
    ];
    expect(rankSources(sources, "").map((s) => s.name)).toEqual([
      "alpha",
      "Gamma",
      "beta",
      "zeta",
    ]);
  });

  it("filters by name substring", () => {
    const sources = [{ id: 1, name: "alpha" }, { id: 2, name: "beta" }];
    expect(rankSources(sources, "alph").map((s) => s.name)).toEqual(["alpha"]);
  });

  it("does not mutate the input array", () => {
    const sources = [{ id: 1, name: "b" }, { id: 2, name: "a" }];
    rankSources(sources, "");
    expect(sources.map((s) => s.name)).toEqual(["b", "a"]);
  });

  it("caps results at 50", () => {
    const sources = Array.from({ length: 60 }, (_, i) => ({
      id: i,
      name: `source-${String(i).padStart(2, "0")}`,
    }));
    expect(rankSources(sources, "")).toHaveLength(50);
  });
});
