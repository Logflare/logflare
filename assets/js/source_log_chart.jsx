import * as _ from "lodash"
import React from "react"
import { ResponsiveBar } from "@nivo/bar"

import { ScaleLoader, BarLoader } from "react-spinners"

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

const LogEventsChart = ({ data, loading }) => {
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
        <ResponsiveBar
          data={data}
          margin={{ top: 20, right: 0, bottom: 20, left: 0 }}
          padding={0.3}
          enableGridY={true}
          indexBy={"timestamp"}
          tooltip={tooltipData => {
            const { value, color, indexValue } = tooltipData
            return (
              <div>
                <strong style={{ color }}>
                  Timestamp: {indexValue}
                </strong>
                <br/>
                <strong style={{ color }}>
                  Value: {value}
                </strong>
              </div>
            )
          }}
          colors={"#5eeb8f"}
          axisTop={null}
          axisRight={null}
          axisBottom={null}
          axisLeft={null}
          enableLabel={false}
          animate={true}
          motionStiffness={90}
          motionDamping={15}
          theme={theme}
        />
      )}
    </div>
  )
}

export { LogEventsChart }
