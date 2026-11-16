/** @vitest-environment jsdom */

import React, { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

globalThis.IS_REACT_ACT_ENVIRONMENT = true;

vi.mock("recharts", async (importOriginal) => {
  const actual = await importOriginal();
  const { cloneElement } = await import("react");

  return {
    ...actual,
    ResponsiveContainer: ({ children }) =>
      cloneElement(children, { width: 300, height: 100 }),
  };
});

import { LogEventsChart } from "../LogEventsChart.jsx";

const chartData = [
  { datetime: "2026-08-14T10:00:00Z", timestamp: "10:00", value: 1 },
  { datetime: "2026-08-14T10:01:00Z", timestamp: "10:01", value: 1 },
  { datetime: "2026-08-14T10:02:00Z", timestamp: "10:02", value: 1 },
];
const aggregateSeries = [
  { key: "aggregate_0", label: "sum(m.latency)" },
  { key: "aggregate_1", label: "avg(m.latency)" },
  { key: "aggregate_2", label: "max(m.latency)" },
  { key: "aggregate_3", label: "count(*)" },
];
const aggregateData = chartData.map((row, index) => ({
  ...row,
  aggregate_0: [10, "20.5", 30][index],
  aggregate_1: [5, null, 15][index],
  aggregate_2: [10, 20.5, 30][index],
  aggregate_3: [0, 0, 0][index],
}));
const chartBounds = {
  bottom: 100,
  height: 100,
  left: 100,
  right: 400,
  top: 0,
  width: 300,
  x: 100,
  y: 0,
  toJSON: () => ({}),
};

let animationFrames;
let container;
let nextAnimationFrameId;
let root;

const flushAnimationFrames = async () => {
  await act(async () => {
    const queuedFrames = [...animationFrames.values()];
    animationFrames.clear();
    queuedFrames.forEach((callback) => callback(performance.now()));
  });
};

const dispatchMouseEvent = async (element, type, clientX) => {
  await act(async () => {
    element.dispatchEvent(
      new MouseEvent(type, {
        bubbles: true,
        button: 0,
        clientX,
        clientY: 50,
      }),
    );
  });
};

const tooltipText = () =>
  [...container.querySelectorAll(".recharts-tooltip-wrapper strong")].map((row) => row.textContent.trim());

const hoverAt = async (clientX) => {
  await dispatchMouseEvent(container.querySelector(".recharts-wrapper"), "mousemove", clientX);
  await flushAnimationFrames();
};

beforeEach(async () => {
  animationFrames = new Map();
  nextAnimationFrameId = 1;
  container = document.createElement("div");
  document.body.appendChild(container);
  root = createRoot(container);

  vi.spyOn(Element.prototype, "getBoundingClientRect").mockReturnValue(
    chartBounds,
  );
  vi.spyOn(HTMLElement.prototype, "offsetWidth", "get").mockReturnValue(300);
  vi.spyOn(HTMLElement.prototype, "offsetHeight", "get").mockReturnValue(100);
  vi.stubGlobal("requestAnimationFrame", (callback) => {
    const frameId = nextAnimationFrameId++;
    animationFrames.set(frameId, callback);
    return frameId;
  });
  vi.stubGlobal("cancelAnimationFrame", (frameId) => {
    animationFrames.delete(frameId);
  });
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("LogEventsChart with Recharts", () => {
  const renderChart = async (props = {}) => {
    await act(async () => {
      root.render(
        <LogEventsChart
          data={aggregateData}
          series={aggregateSeries}
          loading={false}
          chart_data_shape_id="default"
          chart_period="minute"
          display_timezone="Etc/UTC"
          pushEvent={vi.fn()}
          {...props}
        />,
      );
    });
  };

  it("lists each aggregate in the legend", async () => {
    await renderChart();

    const legend = container.querySelector('[aria-label="Chart aggregates"]');
    expect([...legend.querySelectorAll("button")].map((button) => button.textContent)).toEqual([
      "sum(m.latency)", "avg(m.latency)", "max(m.latency)", "count(*)",
    ]);
  });

  it.each([
    [["en-US"], "987,654,321,012.35", "-123,456,789.57"],
    [["de-DE", "en-US"], "987.654.321.012,35", "-123.456.789,57"],
    [["fr-FR"], "987\u202f654\u202f321\u202f012,35", "-123\u202f456\u202f789,57"],
    [["hi-IN"], "9,87,65,43,21,012.35", "-12,34,56,789.57"],
    [[], "987,654,321,012.35", "-123,456,789.57"],
  ])("formats aggregate tooltip numbers for browser languages %j without losing rounding or nulls", async (languages, positive, negative) => {
    vi.spyOn(navigator, "languages", "get").mockReturnValue(languages);
    await renderChart({
      data: aggregateData.map((row) => ({
        ...row,
        aggregate_0: "987654321012.3509",
        aggregate_2: -123456789.56789,
      })),
    });
    await hoverAt(250);

    const tooltip = container.querySelector(".recharts-tooltip-wrapper");
    expect(tooltip.style.visibility).toBe("visible");
    expect(tooltipText()).toEqual([
      `sum(m.latency) ${positive}`,
      "avg(m.latency) —",
      `max(m.latency) ${negative}`,
      "count(*) 0",
      "10:01",
    ]);
  });

  it("keeps every aggregate in the shared tooltip even when the whole bucket is null", async () => {
    await renderChart({
      data: aggregateData.map((row) => ({
        ...row,
        ...Object.fromEntries(aggregateSeries.map(({ key }) => [key, null])),
      })),
    });
    await hoverAt(250);

    const tooltip = container.querySelector(".recharts-tooltip-wrapper");
    expect(tooltip.style.visibility).toBe("visible");
    expect(tooltipText()).toEqual([
      ...aggregateSeries.map(({ label }) => `${label} —`),
      "10:01",
    ]);
  });

  it.each([
    [undefined, "Value"],
    [[], "Value"],
    [[{ key: "value", label: "max(m.response_bytes)" }], "max(m.response_bytes)"],
  ])("retains the single-value tooltip without a multi-aggregate legend for series %j", async (series, label) => {
    await renderChart({ data: chartData.map((row) => ({ ...row, value: 12345.5 })), series });
    expect(container.querySelector('[aria-label="Chart aggregates"]')).toBeNull();
    await hoverAt(250);
    expect(tooltipText()).toEqual([
      `${label} 12,345.50`,
      "10:01",
    ]);
  });

  it.each([
    ["cloudflare_status_codes", { total: 3, status_2xx: 2, status_5xx: 1 }, [
      "Total 3", "5xx 1", "4xx", "3xx", "2xx 2", "1xx", "Other", "10:01",
    ]],
    ["elixir_logger_levels", { total: 3, level_info: 2, other: 1 }, [
      "Total 3", "Emergency", "Critical", "Alert", "Error", "Warn",
      "Notice", "Info 2", "Debug", "Other 1", "10:01",
    ]],
  ])("retains %s tooltip values followed by the timestamp", async (shape, values, expected) => {
    await renderChart({
      series: [{ key: "value", label: "count(*)" }],
      chart_data_shape_id: shape,
      data: chartData.map((row) => ({ ...row, ...values })),
    });
    await hoverAt(250);
    expect(tooltipText()).toEqual(expected);
  });

  it.each([
    [175, "10:00", "t:2026-08-14T10:00:00..2026-08-14T10:01:00"],
    [190, "10:01", "t:2026-08-14T10:01:00..2026-08-14T10:02:00"],
    [310, "10:01", "t:2026-08-14T10:01:00..2026-08-14T10:02:00"],
    [325, "10:01", "t:2026-08-14T10:01:00..2026-08-14T10:02:00"],
    [326, "10:02", "t:2026-08-14T10:02:00..2026-08-14T10:03:00"],
  ])("keeps the hovered bucket and click interval aligned at x=%i", async (clientX, timestamp, querystring) => {
    const pushEvent = vi.fn();
    await renderChart({ pushEvent });
    const chart = container.querySelector(".recharts-wrapper");
    await hoverAt(clientX);
    expect(tooltipText().at(-1)).toBe(timestamp);

    await dispatchMouseEvent(chart, "mousedown", clientX);
    await flushAnimationFrames();
    await dispatchMouseEvent(chart, "mouseup", clientX);
    await flushAnimationFrames();
    await dispatchMouseEvent(chart, "click", clientX);
    await flushAnimationFrames();
    expect(pushEvent.mock.calls).toEqual([
      ["soft_pause", {}],
      ["datetime_update", { querystring, period: "second" }],
    ]);
  });
});
