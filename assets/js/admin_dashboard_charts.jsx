import React from "react"
import {ResponsiveBarCanvas} from "@nivo/bar"

const brandLightBlack = "#1d1d1d"
const brandGreen = "#5eeb8f"


const theme = {
  grid: {
    line: {
      stroke: brandLightBlack,
      strokeWidth: 2,
      strokeDasharray: "4 4",
    },
  },
  axis: {ticks: {text: {fill: brandGreen}}},

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

const Chart = ({data, keys}) => {
  return (
    <div
      style={{
        height: 200,
      }}
    >
      <ResponsiveBarCanvas
        data={data}
        margin={{top: 30, right: 30, bottom: 30, left: 30}}
        padding={0.3}
        enableGridY={true}
        keys={keys}
        indexBy={"timestamp"}
        tooltip={renderDefaultTooltip}
        axisTop={null}
        axisRight={null}
        axisBottom={null}
        enableLabel={false}
        motionStiffness={90}
        motionDamping={15}
        theme={theme}
        colors={(_) => brandGreen}
      />
    </div>
  )
}

export default Chart
