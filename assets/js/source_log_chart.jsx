import React from "react"
import {ResponsiveBarCanvas} from "@nivo/bar"

import {BarLoader} from "react-spinners"

const brandLightBlack = "#1d1d1d"
const brandGray = "#9a9a9a"
const brandGreen = "#5eeb8f"

const warnColor = "#f1ba58"
const errorColor = "#dc3545"
const debugColor = "#8e6ddf"
const infoColor = "#5eeb8f"
const secondInfoColor = "#6286db"

const theme = {
  grid: {
    line: {
      stroke: brandLightBlack,
      strokeWidth: 2,
      strokeDasharray: "4 4",
    },
  },
  tooltip: {
    container: {
      background: brandLightBlack,
    },
  },
}

const renderDefaultTooltip = ({value, color, indexValue}) => {
  return (
    <div>
      <strong style={{color}}>Timestamp: {indexValue}</strong>
      <br/>
      <strong style={{color}}>Value: {value}</strong>
    </div>
  )
}

const renderCfStatusCodeTooltip = ({data, color}) => {
  return (
    <div>
      <strong style={{color: brandGray}}>Timestamp: {data.timestamp}</strong>
      <br/>
      <strong style={{color: brandGray}}>Total: {data.total}</strong>
      <br/>
      <strong style={{color: errorColor}}>5xx: {data.status_5xx}</strong>
      <br/>
      <strong style={{color: warnColor}}>4xx: {data.status_4xx}</strong>
      <br/>
      <strong style={{color: secondInfoColor}}>3xx: {data.status_3xx}</strong>
      <br/>
      <strong style={{color: infoColor}}>2xx: {data.status_2xx}</strong>
      <br/>
      <strong style={{color: debugColor}}>1xx: {data.status_1xx}</strong>
      <br/>
      <strong style={{color: brandGray}}>Other: {data.other}</strong>
    </div>
  )
}

const renderElixirLoggerTooltip = ({data, color}) => {
  return (
    <div>
      <strong style={{color: brandGray}}>Timestamp: {data.timestamp}</strong>
      <br/>
      <strong style={{color: brandGray}}>Total: {data.total}</strong>
      <br/>
      <strong style={{color: errorColor}}>Error: {data.level_error}</strong>
      <br/>
      <strong style={{color: warnColor}}>Warn: {data.level_warn}</strong>
      <br/>
      <strong style={{color: infoColor}}>Info: {data.level_info}</strong>
      <br/>
      <strong style={{color: debugColor}}>Debug: {data.level_debug}</strong>
      <br/>
      <strong style={{color: brandGray}}>Other: {data.other}</strong>
    </div>
  )
}

const tooltipFactory = dataShape => {
  switch (dataShape) {
    case "elixir_logger_levels":
      return renderElixirLoggerTooltip
    case "cloudflare_status_codes":
      return renderCfStatusCodeTooltip
    default:
      return renderDefaultTooltip
  }
}

const chartSettings = (type) => {
  switch (type) {
    case "elixir_logger_levels":
      return {
        colors: ({id}) => {
          const color = {
            level_info: infoColor,
            level_error: errorColor,
            level_warn: warnColor,
            level_debug: debugColor,
            other: brandGray
          }[id]
          return color || brandGray
        },
        keys: ["level_info", "level_debug", "level_error", "level_warn", "other"]
      }

    case "cloudflare_status_codes":
      return {
        colors: ({id}) => {
          const color = {
            status_5xx: errorColor,
            status_4xx: warnColor,
            status_3xx: secondInfoColor,
            status_2xx: infoColor,
            status_1xx: debugColor,
            other: brandGray,
          }[id]
          return color || brandGray
        },
        keys: ["status_5xx", "status_4xx", "status_3xx", "status_2xx", "status_1xx", "other"]
      }
    default:
      return {
        colors: (_) => infoColor,
        keys: ["value"]
      }
  }
}


const LogEventsChart = ({data, loading, chart_data_shape_id: chartDataShapeId}) => {
  const renderTooltip = tooltipFactory(chartDataShapeId)
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
          margin={{top: 20, right: 0, bottom: 0, left: 0}}
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
          motionStiffness={90}
          motionDamping={15}
          theme={theme}
          {...chartSettings(chartDataShapeId)}
        />
      )}
    </div>
  )
}

export {LogEventsChart}
