import * as _ from "lodash"
import React from "react"
import { Bar } from "@nivo/bar"

const LogSparklines = ({ data }) => {
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
      return <strong style={{ color }}>
        {indexValue}
      </strong>
    }}
    colors={{ scheme: "pastel1" }}
    axisTop={null}
    axisRight={null}
    axisBottom={null}
    axisLeft={{
      tickSize: 5,
      tickPadding: 5,
      tickRotation: 0,
      legend: "events",
      legendPosition: "middle",
      legendOffset: -40,
    }}
    labelSkipWidth={12}
    labelSkipHeight={12}
    labelTextColor={"white"}
    animate={true}
    motionStiffness={90}
    motionDamping={15}
  />
}

export { LogSparklines }
