import React from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

const brandLightBlack = "#1d1d1d";
const brandGreen = "#5eeb8f";

const DefaultTooltipContent = ({ active, payload, label }) => {
  if (!active || !payload || !payload.length) return null;
  const total = payload.reduce((sum, entry) => sum + (entry.value || 0), 0);
  return (
    <div style={{ backgroundColor: brandLightBlack }}>
      <strong style={{ color: brandGreen }}>Timestamp: {label}</strong>
      <br />
      <strong style={{ color: brandGreen }}>Value: {total}</strong>
    </div>
  );
};

const Chart = ({ data, keys }) => {
  return (
    <div
      style={{
        height: 200,
      }}
    >
      <ResponsiveContainer width="100%" height={200}>
        <BarChart
          data={data}
          margin={{ top: 30, right: 30, bottom: 30, left: 30 }}
        >
          <CartesianGrid
            strokeDasharray="4 4"
            stroke={brandLightBlack}
            strokeWidth={2}
            vertical={false}
          />
          <XAxis dataKey="timestamp" hide={true} />
          <YAxis tick={{ fill: brandGreen }} />
          <Tooltip
            content={<DefaultTooltipContent />}
            cursor={{ fill: "rgba(255,255,255,0.05)" }}
          />
          {keys.map((key) => (
            <Bar
              key={key}
              dataKey={key}
              stackId="stack"
              fill={brandGreen}
              isAnimationActive={true}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
};

export default Chart;
