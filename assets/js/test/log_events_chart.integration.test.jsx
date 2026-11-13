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

  it("renders a compact legend below unstacked lines with eight colors before repeating", async () => {
    const series = [
      ...aggregateSeries,
      ...Array.from({ length: 5 }, (_, index) => ({
        key: `aggregate_${index + 4}`,
        label: `max(m.field_${index + 4})`,
      })),
    ];
    await renderChart({
      series,
      data: aggregateData.map((row, index) => ({
        ...row,
        ...Object.fromEntries(series.slice(4).map(({ key }) => [key, index + 1])),
      })),
    });

    const legend = container.querySelector('[aria-label="Chart aggregates"]');
    expect([...legend.querySelectorAll("li")].map((item) => item.textContent)).toEqual(
      series.map(({ label }) => label),
    );
    expect(legend.compareDocumentPosition(container.querySelector(".recharts-wrapper")) & Node.DOCUMENT_POSITION_PRECEDING).toBeTruthy();
    expect(container.querySelectorAll(".recharts-bar")).toHaveLength(0);
    const gridLines = [...container.querySelectorAll(".recharts-cartesian-grid-horizontal line")];
    expect(gridLines.length).toBeGreaterThan(0);
    expect(gridLines.every((line) => Number(line.getAttribute("y1")) <= chartBounds.height)).toBe(true);
    const lines = [...container.querySelectorAll(".recharts-line-curve")];
    expect(lines.map((line) => line.getAttribute("stroke"))).toEqual([
      "#5eeb8f", "#6286db", "#8e6ddf", "#f1ba58", "#dc3545",
      "#03C03C", "#bd1550", "#b11226", "#5eeb8f",
    ]);
    expect(lines[0].getAttribute("d")).toBe(lines[2].getAttribute("d"));
    expect(lines[1].getAttribute("d").match(/M/g)).toHaveLength(2);
    const isolatedDots = [...container.querySelectorAll(".recharts-line-dots circle")];
    expect(isolatedDots).toHaveLength(2);
    expect(isolatedDots.map((dot) => dot.getAttribute("fill"))).toEqual(["#6286db", "#6286db"]);
    expect(isolatedDots.every((dot) => Number(dot.getAttribute("r")) > 0)).toBe(true);
    expect(Number(isolatedDots[1].getAttribute("cx"))).toBeGreaterThan(Number(isolatedDots[0].getAttribute("cx")));
    expect(Number(isolatedDots[1].getAttribute("cy"))).toBeLessThan(Number(isolatedDots[0].getAttribute("cy")));
  });

  it("marks an isolated zero without marking null buckets or continuous lines", async () => {
    await renderChart({
      data: aggregateData.map((row, index) => ({
        ...row,
        aggregate_1: [null, 0, null][index],
      })),
    });

    const dots = [...container.querySelectorAll(".recharts-line-dots circle")];
    expect(dots).toHaveLength(1);
    expect(dots[0].getAttribute("fill")).toBe("#6286db");
    expect(Number(dots[0].getAttribute("r"))).toBeGreaterThan(0);
  });

  it("toggles legend series and tooltip rows locally while retaining each series color", async () => {
    const pushEvent = vi.fn();
    await renderChart({ pushEvent });
    const buttons = [...container.querySelectorAll('[aria-label="Chart aggregates"] button')];

    await dispatchMouseEvent(buttons[0], "click", 0);

    expect(buttons[0].getAttribute("aria-pressed")).toBe("false");
    expect(buttons[0].querySelector("span").style.backgroundColor).toBe("rgba(255, 255, 255, 0.08)");
    expect([...container.querySelectorAll(".recharts-line-curve")].map((line) => line.getAttribute("stroke"))).toEqual([
      "#6286db", "#8e6ddf", "#f1ba58",
    ]);

    await hoverAt(250);
    const tooltip = container.querySelector(".recharts-tooltip-wrapper");
    expect(tooltipText()).toEqual([
      "avg(m.latency) —", "max(m.latency) 20.50", "count(*) 0", "10:01",
    ]);
    expect(tooltip.querySelector('[aria-hidden="true"]').style.backgroundColor).toBe("rgb(98, 134, 219)");

    for (const button of buttons.slice(1)) {
      await dispatchMouseEvent(button, "click", 0);
    }
    expect(container.querySelectorAll(".recharts-line-curve")).toHaveLength(0);
    expect(container.querySelectorAll(".recharts-line-dots circle")).toHaveLength(0);
    await hoverAt(250);
    expect(tooltipText()).toEqual([]);

    await dispatchMouseEvent(buttons[1], "click", 0);
    expect(buttons[1].getAttribute("aria-pressed")).toBe("true");
    expect(buttons[1].querySelector("span").style.backgroundColor).toBe("rgb(98, 134, 219)");
    expect([...container.querySelectorAll(".recharts-line-curve")].map((line) => line.getAttribute("stroke"))).toEqual(["#6286db"]);
    expect(container.querySelectorAll(".recharts-line-dots circle")).toHaveLength(2);
    await hoverAt(250);
    expect(tooltipText()).toEqual([
      "avg(m.latency) —", "10:01",
    ]);
    expect(tooltip.querySelector('[aria-hidden="true"]')).toBeNull();
    expect(pushEvent.mock.calls).toEqual([]);
  });

  it.each([
    ["default", aggregateSeries, ["#5eeb8f", "#6286db", "#8e6ddf", "#f1ba58"]],
    ["default", [], []],
    ["cloudflare_status_codes", [], ["#9a9a9a", "#dc3545", "#f1ba58", "#6286db", "#5eeb8f", "#8e6ddf", "#9a9a9a"]],
    ["vercel_status_codes", [], ["#9a9a9a", "#dc3545", "#f1ba58", "#6286db", "#5eeb8f", "#8e6ddf", "#9a9a9a"]],
    ["netlify_status_codes", [], ["#9a9a9a", "#dc3545", "#f1ba58", "#6286db", "#5eeb8f", "#8e6ddf", "#9a9a9a"]],
    ["elixir_logger_levels", [], ["#9a9a9a", "#b11226", "#bd1550", "#dc3545", "#dc3545", "#f1ba58", "#03c03c", "#5eeb8f", "#8e6ddf", "#9a9a9a"]],
  ])("uses gray labels, white values and dots only for multiple tooltip items in %s with series %j", async (shape, series, colors) => {
    await renderChart({ chart_data_shape_id: shape, series });
    await hoverAt(250);

    const tooltip = container.querySelector(".recharts-tooltip-wrapper");
    const rows = [...tooltip.querySelectorAll("strong")];
    for (const row of rows.slice(0, -1)) {
      expect(row.children[0].style.color).toBe("rgb(154, 154, 154)");
      expect(row.children[1].style.color).toBe("rgb(255, 255, 255)");
      expect(row.children[1].style.textAlign).toBe("right");
    }
    expect(rows.at(-1).style.color).toBe("rgb(154, 154, 154)");
    expect([...tooltip.querySelectorAll('[aria-hidden="true"]')].map((dot) => dot.style.backgroundColor)).toEqual(
      colors.map((color) => {
        const element = document.createElement("span");
        element.style.backgroundColor = color;
        return element.style.backgroundColor;
      }),
    );
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
  ])("retains single-value bars without a multi-aggregate legend for series %j", async (series, label) => {
    await renderChart({ data: chartData.map((row) => ({ ...row, value: 12345.5 })), series });
    expect(container.querySelectorAll(".recharts-bar")).toHaveLength(1);
    expect(container.querySelectorAll(".recharts-line")).toHaveLength(0);
    expect(container.querySelector('[aria-label="Chart aggregates"]')).toBeNull();
    await hoverAt(250);
    expect(tooltipText()).toEqual([
      `${label} 12,345.50`,
      "10:01",
    ]);
  });

  it.each([
    ["cloudflare_status_codes", { total: 3, status_2xx: 2, status_5xx: 1 }, 7, [
      "Total 3", "5xx 1", "4xx", "3xx", "2xx 2", "1xx", "Other", "10:01",
    ]],
    ["elixir_logger_levels", { total: 3, level_info: 2, other: 1 }, 10, [
      "Total 3", "Emergency", "Critical", "Alert", "Error", "Warn",
      "Notice", "Info 2", "Debug", "Other 1", "10:01",
    ]],
  ])("retains %s bars and tooltip values followed by the timestamp", async (shape, values, barCount, expected) => {
    await renderChart({
      series: [{ key: "value", label: "count(*)" }],
      chart_data_shape_id: shape,
      data: chartData.map((row) => ({ ...row, ...values })),
    });
    expect(container.querySelectorAll(".recharts-bar")).toHaveLength(barCount);
    expect(container.querySelectorAll(".recharts-line")).toHaveLength(0);
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

  it.each([undefined, aggregateSeries])("keeps a drag-selected range after Recharts delivers the deferred click with series %j", async (series) => {
    const pushEvent = vi.fn();

    await renderChart({ data: series ? aggregateData : chartData, series, pushEvent });

    const chart = container.querySelector(".recharts-wrapper");

    const rangeUpdateCalls = [
      ["soft_pause", {}],
      [
        "datetime_update",
        { querystring: series
          ? "t:2026-08-14T10:00:00..2026-08-14T10:02:00"
          : "t:2026-08-14T10:00:00..2026-08-14T10:03:00" },
      ],
    ];

    await dispatchMouseEvent(chart, "mousemove", 150);
    await flushAnimationFrames();
    expect(tooltipText().at(-1)).toBe("10:00");
    await dispatchMouseEvent(chart, "mousedown", 150);
    await flushAnimationFrames();
    await dispatchMouseEvent(chart, "mousemove", 310);
    await flushAnimationFrames();
    expect(tooltipText().at(-1)).toBe(series ? "10:01" : "10:02");
    await dispatchMouseEvent(chart, "mouseup", 310);

    await flushAnimationFrames();
    expect(pushEvent.mock.calls).toEqual(rangeUpdateCalls);

    // Queue the browser's click before the next frame. In the buggy ordering,
    // the mouseup callback's cleanup ran before Recharts delivered this click.
    await dispatchMouseEvent(chart, "click", 310);

    await flushAnimationFrames();
    expect(pushEvent.mock.calls).toEqual(rangeUpdateCalls);
  });
});
