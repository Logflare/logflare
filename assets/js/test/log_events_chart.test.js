import { describe, expect, it } from "vitest";

import {
  activeDatetime,
  buildTimeRangeQuery,
  chartPointAtX,
} from "../LogEventsChart.jsx";

describe("buildTimeRangeQuery", () => {
  it("includes the complete final selected chart bucket", () => {
    expect(
      buildTimeRangeQuery(
        "2026-08-14T10:00:00Z",
        "2026-08-14T10:05:00Z",
        "Etc/UTC",
        "minute",
      ),
    ).toBe("t:2026-08-14T10:00:00..2026-08-14T10:06:00");
  });

  it("orders a right-to-left drag chronologically", () => {
    expect(
      buildTimeRangeQuery(
        "2026-08-14T10:05:00Z",
        "2026-08-14T10:00:00Z",
        "Etc/UTC",
        "minute",
      ),
    ).toBe("t:2026-08-14T10:00:00..2026-08-14T10:06:00");
  });

  it("uses the configured display timezone", () => {
    expect(
      buildTimeRangeQuery(
        "2026-08-14T10:00:00Z",
        "2026-08-14T10:05:00Z",
        "America/New_York",
        "minute",
      ),
    ).toBe("t:2026-08-14T06:00:00..2026-08-14T06:06:00");
  });

  it("rejects invalid timestamps", () => {
    expect(
      buildTimeRangeQuery(
        "invalid",
        "2026-08-14T10:05:00Z",
        "Etc/UTC",
        "minute",
      ),
    ).toBeNull();
  });
});

describe("activeDatetime", () => {
  it("prefers the active payload datetime", () => {
    expect(
      activeDatetime({
        activeLabel: "fallback",
        activePayload: [{ payload: { datetime: "2026-08-14T10:00:00Z" } }],
      }),
    ).toBe("2026-08-14T10:00:00Z");
  });

  it("maps the formatted active label back to the chart datetime", () => {
    expect(
      activeDatetime(
        { activeIndex: 1, activeLabel: "Fri Aug 14 2026 10:00:00" },
        [
          { datetime: "2026-08-14T09:59:00Z", timestamp: "Fri Aug 14 2026 09:59:00" },
          { datetime: "2026-08-14T10:00:00Z", timestamp: "Fri Aug 14 2026 10:00:00" },
        ],
      ),
    ).toBe("2026-08-14T10:00:00Z");
  });

  it("does not treat a null active index as the first chart point", () => {
    expect(
      activeDatetime(
        { activeIndex: null },
        [{ datetime: "2026-08-14T10:00:00Z", timestamp: "first" }],
      ),
    ).toBeUndefined();
  });

  it("falls back to an ISO active label when chart data is unavailable", () => {
    expect(activeDatetime({ activeLabel: "2026-08-14T10:00:00Z" })).toBe(
      "2026-08-14T10:00:00Z",
    );
  });
});

describe("chartPointAtX", () => {
  const chartData = [
    { datetime: "2026-08-14T10:00:00Z", timestamp: "10:00" },
    { datetime: "2026-08-14T10:01:00Z", timestamp: "10:01" },
    { datetime: "2026-08-14T10:02:00Z", timestamp: "10:02" },
  ];
  const bounds = { left: 100, width: 300 };

  it("maps the pointer position to the matching chart bucket", () => {
    expect(chartPointAtX(250, bounds, chartData)).toEqual({
      label: "10:01",
      datetime: "2026-08-14T10:01:00Z",
    });
  });

  it("rejects pointer positions outside the chart", () => {
    expect(chartPointAtX(99, bounds, chartData)).toBeNull();
    expect(chartPointAtX(401, bounds, chartData)).toBeNull();
  });
});
