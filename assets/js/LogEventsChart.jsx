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
} from "recharts";

import { BarLoader } from "react-spinners";

const brandLightBlack = "#1d1d1d";
const brandGray = "#9a9a9a";
const brandGreen = "#5eeb8f";
const chartGridLineColor = "rgba(255,255,255,0.08)";
const chartHorizontalPoints = [20, 33, 45, 58, 70, 83, 95, 108, 120];

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
        },
        keys: [
          "status_2xx",
          "status_1xx",
          "status_3xx",
          "status_4xx",
          "status_5xx",
          "other",
        ],
      };

    default:
      return {
        colors: { value: infoColor },
        keys: ["value"],
      };
  }
};

const periods = ["day", "hour", "minute", "second"];

const LogEventsChart = ({
  data,
  loading,
  chart_data_shape_id: chartDataShapeId,
  chart_period: chartPeriod,
  display_timezone: userTz,
  pushEvent,
}) => {
  const tz = userTz || "Etc/UTC";
  const triggerTimeSearch = (utcDatetime) => {
    if (!utcDatetime) return;

    pushEvent("soft_pause", {});

    const start = DateTime.fromISO(utcDatetime, { zone: tz }).toISO({
      includeOffset: false,
      suppressMilliseconds: true,
      format: "extended",
    });
    const end = DateTime.fromISO(utcDatetime, { zone: tz })
      .plus({ [chartPeriod + "s"]: 1 })
      .toISO({
        includeOffset: false,
        suppressMilliseconds: true,
        format: "extended",
      });
    const ts = `t:${start}..${end}`;
    const index = periods.findIndex((p) => p === chartPeriod);
    const newPeriod = index === 3 ? periods[3] : periods[index + 1];

    pushEvent("datetime_update", {
      querystring: ts,
      period: newPeriod,
    });
  };

  const handleBarClick = (entry) => {
    const payload = entry?.payload;
    if (!payload) return;
    const utcDatetime = payload.datetime || payload.timestamp;
    triggerTimeSearch(utcDatetime);
  };

  const TooltipContent = tooltipFactory(chartDataShapeId);
  const settings = chartSettings(chartDataShapeId);

  return (
    <div
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
            data={data}
            margin={{ top: 20, right: 0, bottom: 0, left: 0 }}
            style={{ cursor: "pointer" }}
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
            {settings.keys.map((key) => (
              <Bar
                key={key}
                dataKey={key}
                stackId="stack"
                fill={settings.colors[key]}
                isAnimationActive={false}
                onClick={handleBarClick}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
};

export { LogEventsChart };
