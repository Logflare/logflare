import $ from "jquery";
import React from "react";
import { DateTime } from "luxon";
import { ResponsiveBarCanvas } from "@nivo/bar";

import { BarLoader } from "react-spinners";

const brandLightBlack = "#1d1d1d";
const brandGray = "#9a9a9a";
const brandGreen = "#5eeb8f";

const warnColor = "#f1ba58";
const criticalColor = "#bd1550";
const emergencyColor = "#b11226";
const alertColor = "#dc3545";
const errorColor = "#dc3545";
const debugColor = "#8e6ddf";
const noticeColor = "#03C03C";
const infoColor = "#5eeb8f";
const secondInfoColor = "#6286db";

const theme = {
  grid: {
    line: {
      stroke: brandLightBlack,
      strokeWidth: 2,
      strokeDasharray: "4 4",
    },
  },
};

const renderDefaultTooltip = ({ value, color, indexValue }) => {
  return (
    <div style={{ backgroundColor: brandLightBlack, padding: "6px" }}>
      <strong style={{ color }}>Timestamp: {indexValue}</strong>
      <br />
      <strong style={{ color }}>Value: {value}</strong>
    </div>
  );
};

const renderCfStatusCodeTooltip = ({ data, color }) => {
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

const renderElixirLoggerTooltip = ({ data, color }) => {
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
      {tooltips.map(({ c: color, p: property, t }) => {
        return [
          <strong style={{ color }}>
            {t}: {data[property]}
          </strong>,
          <br />,
        ];
      })}
    </div>
  );
};

const tooltipFactory = (dataShape) => {
  switch (dataShape) {
    case "elixir_logger_levels":
      return renderElixirLoggerTooltip;
    case "cloudflare_status_codes":
      return renderCfStatusCodeTooltip;
    case "vercel_status_codes":
      return renderCfStatusCodeTooltip;
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
        colors: ({ id }) => {
          const color = {
            level_info: infoColor,
            level_error: errorColor,
            level_warn: warnColor,
            level_debug: debugColor,
            level_critical: criticalColor,
            level_notice: noticeColor,
            level_alert: alertColor,
            level_emergency: emergencyColor,
            other: brandGray,
          }[id];
          return color || brandGray;
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
      return {
        colors: ({ id }) => {
          const color = {
            status_5xx: errorColor,
            status_4xx: warnColor,
            status_3xx: secondInfoColor,
            status_2xx: infoColor,
            status_1xx: debugColor,
            other: brandGray,
          }[id];
          return color || brandGray;
        },
        keys: [
          "status_5xx",
          "status_4xx",
          "status_3xx",
          "status_2xx",
          "status_1xx",
          "other",
        ],
      };

    case "netlify_status_codes":
      return {
        colors: ({ id }) => {
          const color = {
            status_5xx: errorColor,
            status_4xx: warnColor,
            status_3xx: secondInfoColor,
            status_2xx: infoColor,
            status_1xx: debugColor,
            other: brandGray,
          }[id];
          return color || brandGray;
        },
        keys: [
          "status_5xx",
          "status_4xx",
          "status_3xx",
          "status_2xx",
          "status_1xx",
          "other",
        ],
      };

    case "vercel_status_codes":
      return {
        colors: ({ id }) => {
          const color = {
            status_5xx: errorColor,
            status_4xx: warnColor,
            status_3xx: secondInfoColor,
            status_2xx: infoColor,
            status_1xx: debugColor,
            other: brandGray,
          }[id];
          return color || brandGray;
        },
        keys: [
          "status_5xx",
          "status_4xx",
          "status_3xx",
          "status_2xx",
          "status_1xx",
          "other",
        ],
      };
    default:
      return {
        colors: (_) => infoColor,
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
  const tz = userTz
  const onClick = (event) => {
    pushEvent("soft_pause", {});
    const utcDatetime = event.data.datetime;

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
  const renderTooltip = tooltipFactory(chartDataShapeId);
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
        <ResponsiveBarCanvas
          data={data}
          margin={{ top: 20, right: 0, bottom: 0, left: 0 }}
          padding={0.3}
          enableGridY={true}
          indexBy={"timestamp"}
          tooltip={renderTooltip}
          axisTop={null}
          axisRight={null}
          axisBottom={null}
          axisLeft={null}
          enableLabel={false}
          animate={true}
          onClick={onClick}
          motionStiffness={90}
          motionDamping={15}
          theme={theme}
          {...chartSettings(chartDataShapeId)}
        />
      )}
    </div>
  );
};

export { LogEventsChart };
