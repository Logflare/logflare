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
      }),
    );
  });
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
  it("keeps a drag-selected range after Recharts delivers the deferred click", async () => {
    const pushEvent = vi.fn();

    await act(async () => {
      root.render(
        <LogEventsChart
          data={chartData}
          loading={false}
          chart_data_shape_id="default"
          chart_period="minute"
          display_timezone="Etc/UTC"
          pushEvent={pushEvent}
        />,
      );
    });

    const chart = container.querySelector(".recharts-wrapper");
    expect(chart).not.toBeNull();

    const rangeUpdateCalls = [
      ["soft_pause", {}],
      [
        "datetime_update",
        { querystring: "t:2026-08-14T10:00:00..2026-08-14T10:03:00" },
      ],
    ];

    await dispatchMouseEvent(chart, "mousedown", 150);
    await flushAnimationFrames();
    await dispatchMouseEvent(chart, "mousemove", 350);
    await flushAnimationFrames();
    await dispatchMouseEvent(chart, "mouseup", 350);

    expect(pushEvent).not.toHaveBeenCalled();

    await flushAnimationFrames();
    expect(pushEvent.mock.calls).toEqual(rangeUpdateCalls);

    // Queue the browser's click before the next frame. In the buggy ordering,
    // the mouseup callback's cleanup ran before Recharts delivered this click.
    await dispatchMouseEvent(chart, "click", 350);
    expect(pushEvent.mock.calls).toEqual(rangeUpdateCalls);

    await flushAnimationFrames();
    expect(pushEvent.mock.calls).toEqual(rangeUpdateCalls);
  });
});
