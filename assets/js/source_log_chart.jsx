import * as _ from "lodash"
import React from "react"
import {
  Sparkline,
  BarSeries,
  LineSeries,
  HorizontalReferenceLine,
  PointSeries,
  WithTooltip,
} from "@data-ui/sparkline"
import { allColors } from "@data-ui/theme" // open-color colors

const renderTooltip = ({ event, datum, data, color }) => {
  return <div>
    <strong style={{ color }}>{datum.label}</strong>
    <div>
      <strong>Timestamp </strong>
      {datum.timestamp}
    </div>
    <div>
      <strong>Value </strong>
      {datum.value}
    </div>
  </div>
}

const LogSparklines = ({ data }) => {
  const width = innerWidth * 0.9
  return <WithTooltip renderTooltip={renderTooltip}>
    {({ onMouseMove, onMouseLeave, tooltipData }) => (
      <Sparkline
        onMouseLeave={onMouseLeave}
        onMouseMove={onMouseMove}
        ariaLabel="Log events count"
        margin={{ top: 50, right: 20, bottom: 50, left: 20 }}
        width={width}
        height={150}
        data={data}
        valueAccessor={x => x.value}
      >
        <LineSeries
          stroke={allColors.green[7]}
        />
      </Sparkline>
    )}
  </WithTooltip>
}

export { LogSparklines }
