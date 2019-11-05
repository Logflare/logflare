import * as _ from "lodash"
import React from "react"
import {
  Sparkline,
  LineSeries,
  HorizontalReferenceLine,
  PointSeries,
} from "@data-ui/sparkline"
import { allColors } from "@data-ui/theme" // open-color colors

const LogSparklines = ({ data }) => {
  return <Sparkline
    ariaLabel="Log events count"
    margin={{ top: 50, right: 20, bottom: 50, left: 20 }}
    width={innerWidth}
    height={100}
    data={data}
    valueAccessor={_.property("count")}
  >
    <HorizontalReferenceLine
      stroke={allColors.green[8]}
      strokeWidth={1}
      strokeDasharray="4 4"
      reference="median"
    />
    <LineSeries
      showArea={false}
      stroke={allColors.green[7]}
    />
    <PointSeries
      points={["min", "max"]}
      fill={allColors.green[3]}
      size={5}
      stroke="#000"
      renderLabel={val => val.toFixed(2)}
    />
  </Sparkline>
}

export { LogSparklines }
