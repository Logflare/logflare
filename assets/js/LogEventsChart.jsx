import React from "react";
import { DateTime } from "luxon";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
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
const chartHorizontalPoints = [20, 33, 45, 58, 70, 83, 95];
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
const aggregateColors = [
  infoColor,
  secondInfoColor,
  debugColor,
  warnColor,
  errorColor,
  noticeColor,
  criticalColor,
  emergencyColor,
];

const renderIsolatedAggregateDot = ({ cx, cy, index, points, stroke, value }) => {
  if (value == null || points[index - 1]?.value != null || points[index + 1]?.value != null) {
    return null;
  }

  return <circle cx={cx} cy={cy} r={2.5} fill={stroke} pointerEvents="none" />;
};

const ChartTooltip = ({ active, payload, label, items }) => {
  const numberFormatter = React.useMemo(
    () => new Intl.NumberFormat(navigator.languages.length ? navigator.languages : "en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
      trailingZeroDisplay: "stripIfInteger",
    }),
    [],
  );

  if (!active || !payload?.length || !items.length) {
    return null;
  }
  const data = payload[0].payload;

  return (
    <div style={{ backgroundColor: brandLightBlack, padding: "6px" }}>
      {items.map(({ key, label: itemLabel, color }) => (
        <div key={key} style={{ display: "flex", alignItems: "center", gap: 6 }}>
          {items.length > 1 && (
            <span
              aria-hidden="true"
              style={{
                width: 8,
                height: 8,
                flexShrink: 0,
                borderRadius: "50%",
                backgroundColor: color,
              }}
            />
          )}
          <strong style={{ display: "flex", flex: 1, justifyContent: "space-between", gap: 16 }}>
            <span style={{ color: brandGray }}>{itemLabel} </span>
            <span style={{ color: "#ffffff", textAlign: "right", whiteSpace: "nowrap" }}>
              {data[key] === undefined ? "" : data[key] === null ? "—" : numberFormatter.format(Number(data[key]))}
            </span>
          </strong>
        </div>
      ))}
      <strong style={{ color: brandGray, display: "block", lineHeight: 1.5, marginTop: 8 }}>{label}</strong>
    </div>
  );
};

const tooltipItems = (dataShape, series) => {
  switch (dataShape) {
    case "elixir_logger_levels":
      return [
        { key: "total", label: "Total", color: brandGray },
        { key: "level_emergency", label: "Emergency", color: emergencyColor },
        { key: "level_critical", label: "Critical", color: criticalColor },
        { key: "level_alert", label: "Alert", color: alertColor },
        { key: "level_error", label: "Error", color: errorColor },
        { key: "level_warn", label: "Warn", color: warnColor },
        { key: "level_notice", label: "Notice", color: noticeColor },
        { key: "level_info", label: "Info", color: infoColor },
        { key: "level_debug", label: "Debug", color: debugColor },
        { key: "other", label: "Other", color: brandGray },
      ];
    case "cloudflare_status_codes":
    case "vercel_status_codes":
    case "netlify_status_codes":
      return [
        { key: "total", label: "Total", color: brandGray },
        { key: "status_5xx", label: "5xx", color: errorColor },
        { key: "status_4xx", label: "4xx", color: warnColor },
        { key: "status_3xx", label: "3xx", color: secondInfoColor },
        { key: "status_2xx", label: "2xx", color: infoColor },
        { key: "status_1xx", label: "1xx", color: debugColor },
        { key: "other", label: "Other", color: brandGray },
      ];
    default:
      return [{ key: "value", label: series[0]?.label ?? "Value", color: infoColor }];
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

const chartPointAtX = (clientX, bounds, chartData = [], scale = "band") => {
  if (!Number.isFinite(clientX) || !bounds || bounds.width <= 0 || chartData.length === 0) {
    return null;
  }

  const relativeX = clientX - bounds.left;
  if (relativeX < 0 || relativeX > bounds.width) return null;

  const index = scale === "point"
    ? Math.max(0, Math.ceil((relativeX / bounds.width) * (chartData.length - 1) - 0.5))
    : Math.min(
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
  series = [],
  loading,
  chart_data_shape_id: chartDataShapeId,
  chart_period: chartPeriod,
  display_timezone: userTz,
  pushEvent,
}) => {
  const tz = userTz || "Etc/UTC";
  const chartContainerRef = React.useRef(null);
  const [dragRange, setDragRange] = React.useState(null);
  const [hiddenAggregates, setHiddenAggregates] = React.useState([]);
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
      multipleAggregates ? "point" : "band",
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

  const settings = chartSettings(chartDataShapeId);
  const multipleAggregates = series.length > 1;
  const visibleTooltipItems = multipleAggregates
    ? series.map((item, index) => ({
        ...item,
        color: aggregateColors[index % aggregateColors.length],
      })).filter(({ label }) => !hiddenAggregates.includes(label))
    : tooltipItems(chartDataShapeId, series);
  const Chart = multipleAggregates ? LineChart : BarChart;
  const topStackKey = settings.keys[settings.keys.length - 1];
  const chartData = React.useMemo(
    () =>
      data.map((entry) => {
        if (multipleAggregates) {
          return {
            ...entry,
            ...Object.fromEntries(
              series.map(({ key }) => [
                key,
                entry[key] == null ? null : Number(entry[key]),
              ]),
            ),
          };
        }
        const total = barTotal(entry, settings.keys);

        return {
          ...entry,
          hasHighlight: total >= highlightMinValue,
        };
      }),
    [data, settings.keys, multipleAggregates, series],
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
        height: multipleAggregates && !loading ? "auto" : 100,
        display: "flex",
        flexDirection: multipleAggregates ? "column" : "row",
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
          <Chart
            data={chartData}
            margin={{ top: 20, right: 0, bottom: multipleAggregates ? 3 : 0, left: 0 }}
            className={multipleAggregates ? "[&_.recharts-surface]:tw-overflow-visible" : undefined}
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
              filterNull={!multipleAggregates}
              content={<ChartTooltip items={visibleTooltipItems} />}
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
            {multipleAggregates
              ? series.map(({ key, label }, index) => (
                  <Line
                    key={key}
                    dataKey={key}
                    name={label}
                    type="linear"
                    stroke={aggregateColors[index % aggregateColors.length]}
                    strokeWidth={2}
                    dot={renderIsolatedAggregateDot}
                    connectNulls={false}
                    hide={hiddenAggregates.includes(label)}
                    isAnimationActive={false}
                  />
                ))
              : settings.keys.map((key) => (
                  <Bar
                    key={key}
                    dataKey={key}
                    stackId="stack"
                    fill={settings.colors[key]}
                    isAnimationActive={false}
                    minPointSize={minPointSizeForDataKey(chartData, key)}
                    pointerEvents="none"
                    shape={key === topStackKey ? renderBarWithHighlight : undefined}
                  />
                ))}
          </Chart>
        </ResponsiveContainer>
      )}
      {multipleAggregates && !loading && (
        <ul
          aria-label="Chart aggregates"
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: "4px 16px",
            alignSelf: "flex-start",
            listStyle: "none",
            margin: "9.6px 0",
            padding: 0,
            fontSize: 12,
            lineHeight: "16px",
          }}
        >
          {series.map(({ key, label }, index) => (
            <li key={key} style={{ display: "flex" }}>
              <button
                type="button"
                className="tw-text-xs"
                aria-pressed={!hiddenAggregates.includes(label)}
                onClick={() =>
                  setHiddenAggregates((hidden) =>
                    hidden.includes(label)
                      ? hidden.filter((aggregate) => aggregate !== label)
                      : [...hidden, label],
                  )
                }
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                  border: 0,
                  padding: 0,
                  background: "none",
                  font: "inherit",
                  cursor: "pointer",
                }}
              >
                <span
                  aria-hidden="true"
                  style={{
                    width: 8,
                    height: 8,
                    borderRadius: "50%",
                    backgroundColor: hiddenAggregates.includes(label)
                      ? chartGridLineColor
                      : aggregateColors[index % aggregateColors.length],
                  }}
                />
                <span style={{ color: brandGray }}>{label}</span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
};

export { activeDatetime, buildTimeRangeQuery, chartPointAtX, LogEventsChart };
