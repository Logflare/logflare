import React from "react";
import TestRenderer, { act } from "react-test-renderer";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("recharts", async () => {
  const { createElement, useRef } = await import("react");
  const emptyComponent = () => null;
  const mouseEventNames = [
    "onClick",
    "onMouseDown",
    "onMouseLeave",
    "onMouseMove",
    "onMouseUp",
  ];

  return {
    Bar: emptyComponent,
    BarChart: ({ children, ...props }) => {
      const frameIdsByEvent = useRef(new Map());
      const deferredMouseProps = Object.fromEntries(
        mouseEventNames
          .filter((name) => props[name])
          .map((name) => [
            name,
            (...args) => {
              const pendingFrameId = frameIdsByEvent.current.get(name);
              if (pendingFrameId !== undefined) {
                cancelAnimationFrame(pendingFrameId);
              }

              const frameId = requestAnimationFrame(() => {
                props[name](...args);
                frameIdsByEvent.current.delete(name);
              });
              frameIdsByEvent.current.set(name, frameId);
            },
          ]),
      );

      return createElement(
        "bar-chart",
        { ...props, ...deferredMouseProps },
        children,
      );
    },
    CartesianGrid: emptyComponent,
    ReferenceArea: emptyComponent,
    ResponsiveContainer: ({ children }) => children,
    Tooltip: emptyComponent,
    XAxis: emptyComponent,
    YAxis: emptyComponent,
  };
});

import {
  activeDatetime,
  buildTimeRangeQuery,
  chartPointAtX,
  LogEventsChart,
} from "../LogEventsChart.jsx";

const chartData = [
  { datetime: "2026-08-14T10:00:00Z", timestamp: "10:00", value: 1 },
  { datetime: "2026-08-14T10:01:00Z", timestamp: "10:01", value: 1 },
  { datetime: "2026-08-14T10:02:00Z", timestamp: "10:02", value: 1 },
];
const chartBounds = { left: 100, width: 300 };

const renderChart = (pushEvent, data = chartData) =>
  TestRenderer.create(
    React.createElement(LogEventsChart, {
      data,
      loading: false,
      chart_data_shape_id: "default",
      chart_period: "minute",
      display_timezone: "Etc/UTC",
      pushEvent,
    }),
    {
      createNodeMock: (element) =>
        element.type === "div"
          ? { getBoundingClientRect: () => chartBounds }
          : null,
    },
  );

let animationFrames = new Map();
let nextAnimationFrameId = 1;

const flushAnimationFrames = () => {
  act(() => {
    const queuedFrames = [...animationFrames.values()];
    animationFrames.clear();
    queuedFrames.forEach((callback) => callback());
  });
};

beforeEach(() => {
  animationFrames = new Map();
  nextAnimationFrameId = 1;
  vi.stubGlobal("requestAnimationFrame", (callback) => {
    const frameId = nextAnimationFrameId++;
    animationFrames.set(frameId, callback);
    return frameId;
  });
  vi.stubGlobal("cancelAnimationFrame", (frameId) => {
    animationFrames.delete(frameId);
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
});

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
  it("maps the pointer position to the matching chart bucket", () => {
    expect(chartPointAtX(250, chartBounds, chartData)).toEqual({
      label: "10:01",
      datetime: "2026-08-14T10:01:00Z",
    });
  });

  it("rejects pointer positions outside the chart", () => {
    expect(chartPointAtX(99, chartBounds, chartData)).toBeNull();
    expect(chartPointAtX(401, chartBounds, chartData)).toBeNull();
  });
});

describe("LogEventsChart pointer selection", () => {
  it("emits one range update and suppresses the deferred post-drag click", () => {
    const pushEvent = vi.fn();
    let renderer;

    act(() => {
      renderer = renderChart(pushEvent);
    });
    const chart = renderer.root.findByType("bar-chart");

    chart.props.onMouseDown({}, { clientX: 150 });
    flushAnimationFrames();
    chart.props.onMouseMove({}, { clientX: 250 });
    chart.props.onMouseMove({}, { clientX: 350 });
    flushAnimationFrames();
    chart.props.onMouseUp({}, { clientX: 350 });
    flushAnimationFrames();
    flushAnimationFrames();
    chart.props.onClick({}, { clientX: 350 });
    flushAnimationFrames();

    expect(pushEvent.mock.calls).toEqual([
      ["soft_pause", {}],
      ["datetime_update", {
        querystring: "t:2026-08-14T10:00:00..2026-08-14T10:03:00",
      }],
    ]);

    pushEvent.mockClear();
    chart.props.onMouseDown({}, { clientX: 250 });
    flushAnimationFrames();
    chart.props.onMouseUp({}, { clientX: 250 });
    flushAnimationFrames();
    chart.props.onClick({}, { clientX: 250 });
    flushAnimationFrames();

    expect(pushEvent.mock.calls).toEqual([
      ["soft_pause", {}],
      ["datetime_update", {
        querystring: "t:2026-08-14T10:01:00..2026-08-14T10:02:00",
        period: "second",
      }],
    ]);
  });

  it("preserves an ordinary single-bucket click", () => {
    const pushEvent = vi.fn();
    let renderer;

    act(() => {
      renderer = renderChart(pushEvent);
    });
    const chart = renderer.root.findByType("bar-chart");

    chart.props.onMouseDown({}, { clientX: 250 });
    flushAnimationFrames();
    chart.props.onMouseUp({}, { clientX: 250 });
    flushAnimationFrames();
    chart.props.onClick({}, { clientX: 250 });
    flushAnimationFrames();

    expect(pushEvent.mock.calls).toEqual([
      ["soft_pause", {}],
      ["datetime_update", {
        querystring: "t:2026-08-14T10:01:00..2026-08-14T10:02:00",
        period: "second",
      }],
    ]);
  });

  it("ignores clicks when the chart has no active point", () => {
    const pushEvent = vi.fn();
    let renderer;

    act(() => {
      renderer = renderChart(pushEvent, []);
    });
    const chart = renderer.root.findByType("bar-chart");

    chart.props.onClick({}, { clientX: 250 });
    flushAnimationFrames();

    expect(pushEvent).not.toHaveBeenCalled();
  });
});
