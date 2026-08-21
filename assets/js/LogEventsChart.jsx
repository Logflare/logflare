import React from "react";
import { DateTime } from "luxon";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
  ReferenceArea,
} from "recharts";

import { BarLoader } from "react-spinners";

const brandLightBlack = "#1d1d1d";
const brandGray = "#9a9a9a";
const brandGreen = "#5eeb8f";
const chartGridLineColor = "rgba(255,255,255,0.08)";
const chartHorizontalPoints = [20, 33, 45, 58, 70, 83, 95, 108, 120];
const minBarHeight = 2;
const highlightMinValue = 1;
const highlightFill = "rgba(154,154,154,0.1)";

const warnColor = "#f1ba58";
const criticalColor = "#bd1550";
const emergencyColor = "#b11226";
const alertColor = "#dc3545";
const errorColor = "#dc3545";
const debugColor = "#8e6ddf";
const noticeColor = "#03C03C";
const infoColor = "#5eeb8f";
const secondInfoColor = "#6286db";

const renderDefaultTooltip = ({ active, payload, label }) => {
  if (!active || !payload || !payload.length) return null;
  const value = payload[0].value;
  const color = payload[0].fill;
  return (
    <div style={{ backgroundColor: brandLightBlack, padding: "6px" }}>
      <strong style={{ color }}>Timestamp: {label}</strong>
      <br />
      <strong style={{ color }}>Value: {value}</strong>
    </div>
  );
};

const renderCfStatusCodeTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const data = payload[0]?.payload;
  if (!data) return null;
  return (
    <div style={{ backgroundColor: brandLightBlack, padding: "6px" }}>
      <strong style={{ color: brandGray }}>Timestamp: {data.timestamp}</strong>
      <br />
      <strong style={{ color: brandGray }}>Total: {data.total}</strong>
      <br />
      <strong style={{ color: errorColor }}>5xx: {data.status_5xx}</strong>
      <br />
      <strong style={{ color: warnColor }}>4xx: {data.status_4xx}</strong>
      <br />
      <strong style={{ color: secondInfoColor }}>3xx: {data.status_3xx}</strong>
      <br />
      <strong style={{ color: infoColor }}>2xx: {data.status_2xx}</strong>
      <br />
      <strong style={{ color: debugColor }}>1xx: {data.status_1xx}</strong>
      <br />
      <strong style={{ color: brandGray }}>Other: {data.other}</strong>
    </div>
  );
};

const renderElixirLoggerTooltip = ({ active, payload }) => {
  if (!active || !payload || !payload.length) return null;
  const data = payload[0]?.payload;
  if (!data) return null;
  const tooltips = [
    { c: brandGray, p: "timestamp", t: "Timestamp" },
    { c: brandGray, p: "total", t: "Total" },
    { c: emergencyColor, p: "level_emergency", t: "Emergency" },
    { c: criticalColor, p: "level_critical", t: "Critical" },
    { c: alertColor, p: "level_alert", t: "Alert" },
    { c: errorColor, p: "level_error", t: "Error" },
    { c: warnColor, p: "level_warn", t: "Warn" },
    { c: noticeColor, p: "level_notice", t: "Notice" },
    { c: infoColor, p: "level_info", t: "Info" },
    { c: debugColor, p: "level_debug", t: "Debug" },
    { c: brandGray, p: "other", t: "Other" },
  ];
  return (
    <div style={{ backgroundColor: brandLightBlack, padding: "6px" }}>
      {tooltips.map(({ c: color, p: property, t }, index) => (
        <React.Fragment key={property}>
          <strong style={{ color }}>
            {t}: {data[property]}
          </strong>
          {index < tooltips.length - 1 && <br />}
        </React.Fragment>
      ))}
    </div>
  );
};

const tooltipFactory = (dataShape) => {
  switch (dataShape) {
    case "elixir_logger_levels":
      return renderElixirLoggerTooltip;
    case "cloudflare_status_codes":
    case "vercel_status_codes":
    case "netlify_status_codes":
      return renderCfStatusCodeTooltip;
    default:
      return renderDefaultTooltip;
  }
};

const chartSettings = (type) => {
  switch (type) {
    case "elixir_logger_levels":
      return {
        colors: {
          level_info: infoColor,
          level_error: errorColor,
          level_warn: warnColor,
          level_debug: debugColor,
          level_critical: criticalColor,
          level_notice: noticeColor,
          level_alert: alertColor,
          level_emergency: emergencyColor,
          other: brandGray,
          value: brandGray,
        },
        keys: [
          "level_info",
          "level_debug",
          "level_notice",
          "level_critical",
          "level_emergency",
          "level_alert",
          "level_error",
          "level_warn",
          "other",
          "value",
        ],
      };

    case "cloudflare_status_codes":
    case "netlify_status_codes":
    case "vercel_status_codes":
      return {
        colors: {
          status_5xx: errorColor,
          status_4xx: warnColor,
          status_3xx: secondInfoColor,
          status_2xx: infoColor,
          status_1xx: debugColor,
          other: brandGray,
          value: brandGray,
        },
        keys: [
          "status_2xx",
          "status_1xx",
          "status_3xx",
          "status_4xx",
          "status_5xx",
          "other",
          "value",
        ],
      };

    default:
      return {
        colors: { value: infoColor },
        keys: ["value"],
      };
  }
};

const minPointSizeForDataKey = (chartData, dataKey) => (_value, index) =>
  Number(chartData[index]?.[dataKey]) > 0 ? minBarHeight : 0;

const barTotal = (entry, keys) =>
  keys.reduce((sum, key) => sum + Math.max(Number(entry?.[key]) || 0, 0), 0);

const periods = ["day", "hour", "minute", "second"];

const formatSearchDatetime = (utcDatetime, tz) => {
  const datetime = DateTime.fromISO(utcDatetime, { zone: tz });
  if (!datetime.isValid) return null;

  return datetime.toISO({
    includeOffset: false,
    suppressMilliseconds: true,
    format: "extended",
  });
};

const buildTimeRangeQuery = (
  firstUtcDatetime,
  secondUtcDatetime,
  tz,
  chartPeriod,
) => {
  const first = DateTime.fromISO(firstUtcDatetime, { zone: tz });
  const second = DateTime.fromISO(secondUtcDatetime, { zone: tz });
  if (!first.isValid || !second.isValid || !periods.includes(chartPeriod)) return null;

  const [start, lastBucket] =
    first.toMillis() <= second.toMillis() ? [first, second] : [second, first];
  const end = lastBucket.plus({ [`${chartPeriod}s`]: 1 });

  return `t:${formatSearchDatetime(start.toISO(), tz)}..${formatSearchDatetime(end.toISO(), tz)}`;
};

const activeChartPoint = (state, chartData = []) => {
  const activePayload = state?.activePayload?.[0]?.payload;
  const hasActiveIndex = state?.activeIndex !== null && state?.activeIndex !== undefined;
  const activeIndex = hasActiveIndex ? Number(state.activeIndex) : null;
  const indexedPoint = Number.isInteger(activeIndex) ? chartData[activeIndex] : null;
  const activePoint = activePayload || indexedPoint;
  const label = state?.activeLabel || activePoint?.timestamp || activePoint?.datetime;
  const datetime = activePoint?.datetime || activePoint?.timestamp || state?.activeLabel;

  return label && datetime ? { label, datetime } : null;
};

const activeDatetime = (state, chartData = []) =>
  activeChartPoint(state, chartData)?.datetime;

const chartPointAtX = (clientX, bounds, chartData = []) => {
  if (!Number.isFinite(clientX) || !bounds || bounds.width <= 0 || chartData.length === 0) {
    return null;
  }

  const relativeX = clientX - bounds.left;
  if (relativeX < 0 || relativeX > bounds.width) return null;

  const index = Math.min(
    chartData.length - 1,
    Math.floor((relativeX / bounds.width) * chartData.length),
  );
  const point = chartData[index];
  const label = point?.timestamp || point?.datetime;
  const datetime = point?.datetime || point?.timestamp;

  return label && datetime ? { label, datetime } : null;
};

const LogEventsChart = ({
  data,
  loading,
  chart_data_shape_id: chartDataShapeId,
  chart_period: chartPeriod,
  display_timezone: userTz,
  pushEvent,
}) => {
  const tz = userTz || "Etc/UTC";
  const chartContainerRef = React.useRef(null);
  const [dragRange, setDragRange] = React.useState(null);
  const dragStartRef = React.useRef(null);
  const dragEndRef = React.useRef(null);
  const suppressClickRef = React.useRef(false);

  const triggerTimeSearch = (utcDatetime) => {
    if (!utcDatetime) return;

    const start = formatSearchDatetime(utcDatetime, tz);
    const end = DateTime.fromISO(utcDatetime, { zone: tz })
      .plus({ [chartPeriod + "s"]: 1 })
      .toISO({
        includeOffset: false,
        suppressMilliseconds: true,
        format: "extended",
      });
    if (!start || !end) return;

    pushEvent("soft_pause", {});

    const index = periods.findIndex((p) => p === chartPeriod);
    const newPeriod = index === 3 ? periods[3] : periods[index + 1];

    pushEvent("datetime_update", {
      querystring: `t:${start}..${end}`,
      period: newPeriod,
    });
  };

  const triggerTimeRangeSearch = (firstUtcDatetime, secondUtcDatetime) => {
    const querystring = buildTimeRangeQuery(
      firstUtcDatetime,
      secondUtcDatetime,
      tz,
      chartPeriod,
    );
    if (!querystring) return;

    pushEvent("soft_pause", {});
    pushEvent("datetime_update", { querystring });
  };

  const consumeSuppressedClick = () => {
    const suppressClick = suppressClickRef.current;
    suppressClickRef.current = false;
    return suppressClick;
  };
  const pointForMouseEvent = (state, event) =>
    chartPointAtX(
      event?.clientX,
      chartContainerRef.current?.getBoundingClientRect(),
      chartData,
    ) || activeChartPoint(state, chartData);

  const handleChartClick = (state, event) => {
    if (consumeSuppressedClick()) return;
    triggerTimeSearch(pointForMouseEvent(state, event)?.datetime);
  };

  const handleMouseDown = (state, event) => {
    const point = pointForMouseEvent(state, event);
    if (!point) return;

    dragStartRef.current = point;
    dragEndRef.current = point;
    suppressClickRef.current = false;
    setDragRange({ start: point.label, end: point.label });
  };

  const handleMouseMove = (state, event) => {
    if (!dragStartRef.current) return;

    const point = pointForMouseEvent(state, event);
    if (!point) return;

    dragEndRef.current = point;
    setDragRange({ start: dragStartRef.current.label, end: point.label });
  };

  const resetDrag = () => {
    dragStartRef.current = null;
    dragEndRef.current = null;
    setDragRange(null);
  };

  const handleMouseUp = (state, event) => {
    const start = dragStartRef.current;
    const end = pointForMouseEvent(state, event) || dragEndRef.current;
    const didDrag = Boolean(start && end && start.label !== end.label);

    if (didDrag) {
      suppressClickRef.current = true;
      triggerTimeRangeSearch(start.datetime, end.datetime);
    }

    resetDrag();
  };

  const handleMouseLeave = () => {
    if (!dragStartRef.current) return;

    suppressClickRef.current = false;
    resetDrag();
  };

  const TooltipContent = tooltipFactory(chartDataShapeId);
  const settings = chartSettings(chartDataShapeId);
  const topStackKey = settings.keys[settings.keys.length - 1];
  const chartData = React.useMemo(
    () =>
      data.map((entry) => {
        const total = barTotal(entry, settings.keys);

        return {
          ...entry,
          hasHighlight: total >= highlightMinValue,
        };
      }),
    [data, settings.keys],
  );

  const renderBarWithHighlight = ({
    x,
    y,
    width,
    height,
    fill,
    background,
    payload,
  }) => {
    const top = background?.y ?? 0;
    const highlightHeight = y - top;
    const showHighlight =
      payload?.hasHighlight &&
      Number.isFinite(highlightHeight) &&
      highlightHeight > 0;

    return (
      <g>
        {showHighlight && (
          <rect
            x={x}
            y={top}
            width={width}
            height={highlightHeight}
            fill={highlightFill}
            pointerEvents="none"
          />
        )}
        <rect
          x={x}
          y={y}
          width={width}
          height={height}
          fill={fill}
          pointerEvents="none"
        />
      </g>
    );
  };

  return (
    <div
      ref={chartContainerRef}
      style={{
        height: 100,
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
      }}
    >
      {loading ? (
        <div>
          <BarLoader
            height={5}
            width={400}
            radius={0}
            margin={2}
            color={brandGreen}
            loading={loading}
          />
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={100}>
          <BarChart
            data={chartData}
            margin={{ top: 20, right: 0, bottom: 0, left: 0 }}
            style={{ cursor: dragRange ? "crosshair" : "pointer" }}
            onClick={handleChartClick}
            onMouseDown={handleMouseDown}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
            onMouseLeave={handleMouseLeave}
          >
            <CartesianGrid
              stroke={chartGridLineColor}
              strokeWidth={1}
              horizontalPoints={chartHorizontalPoints}
              vertical={false}
            />
            <XAxis dataKey="timestamp" hide={true} />
            <YAxis hide={true} />
            <Tooltip
              isAnimationActive={false}
              wrapperStyle={{ zIndex: 500 }}
              content={<TooltipContent />}
              cursor={{ fill: "rgba(255,255,255,0.05)" }}
            />
            {dragRange?.start !== dragRange?.end && (
              <ReferenceArea
                x1={dragRange.start}
                x2={dragRange.end}
                fill={brandGreen}
                fillOpacity={0.15}
                stroke={brandGreen}
                strokeOpacity={0.5}
                ifOverflow="visible"
                style={{ pointerEvents: "none" }}
              />
            )}
            {settings.keys.map((key) => (
              <Bar
                key={key}
                dataKey={key}
                stackId="stack"
                fill={settings.colors[key]}
                isAnimationActive={false}
                minPointSize={minPointSizeForDataKey(chartData, key)}
                shape={key === topStackKey ? renderBarWithHighlight : undefined}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
};

export { activeDatetime, buildTimeRangeQuery, chartPointAtX, LogEventsChart };
