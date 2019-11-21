import * as _ from "lodash"
import React from "react"
import { Bar } from "@nivo/bar"

const brandLightBlack = "#1d1d1d"
const brandGray = "#9a9a9a"
const brandGreen = "#5eeb8f"

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

const LogEventsChart = ({ data }) => {
  const width = innerWidth * 0.9
  return <Bar
    data={data}
    width={width}
    height={200}
    margin={{ top: 50, right: 60, bottom: 50, left: 60 }}
    padding={0.3}
    enableGridY={true}
    indexBy={"timestamp"}
    tooltip={(tooltipData) => {
      const { value, color, indexValue } = tooltipData
      return <div>
        <strong style={{ color }}>
          Timestamp: {indexValue}
        </strong>
        <br/>
        <strong style={{ color }}>
          Value: {value}
        </strong>
      </div>
    }}
    colors={"#5eeb8f"}
    axisTop={null}
    axisRight={null}
    axisBottom={null}
    axisLeft={null}
    labelSkipWidth={12}
    labelSkipHeight={12}
    labelTextColor={"white"}
    animate={true}
    motionStiffness={90}
    motionDamping={15}
    theme={theme}
  />
}

export { LogEventsChart }
