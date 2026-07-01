import React, { useState } from "react";
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  ComposedChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";

const TOOLTIP = {
  backgroundColor: "#1e1e2e",
  border: "1px solid #313244",
  borderRadius: "6px",
  color: "#cdd6f4",
  fontSize: 12,
};

const AXIS = { fill: "#6c7086", fontSize: 11 };
const GRID = "#313244";

function Panel({ title, children }) {
  return (
    <div
      style={{
        background: "#1e1e2e",
        border: "1px solid #313244",
        borderRadius: "8px",
        padding: "16px",
      }}
    >
      <div
        style={{
          color: "#6c7086",
          fontSize: 11,
          fontWeight: 600,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          marginBottom: 12,
        }}
      >
        {title}
      </div>
      {children}
    </div>
  );
}

function StatCard({ label, value, color, unit = "" }) {
  return (
    <div
      style={{
        background: "#1e1e2e",
        border: "1px solid #313244",
        borderRadius: "8px",
        padding: "14px 20px",
        display: "flex",
        flexDirection: "column",
        gap: 4,
      }}
    >
      <div style={{ color: "#6c7086", fontSize: 11, letterSpacing: "0.05em", textTransform: "uppercase" }}>
        {label}
      </div>
      <div style={{ color, fontSize: 28, fontWeight: 700, fontFamily: "monospace", lineHeight: 1 }}>
        {typeof value === "number" ? value.toLocaleString() : value}
        {unit && <span style={{ fontSize: 14, marginLeft: 4, color: "#6c7086" }}>{unit}</span>}
      </div>
    </div>
  );
}

const MODE_COLORS = {
  producer: { bg: "#a6e3a1", text: "#1e1e2e" },
  consumer: { bg: "#89b4fa", text: "#1e1e2e" },
  both:     { bg: "#cba6f7", text: "#1e1e2e" },
  none:     { bg: "#313244", text: "#6c7086" },
};

const GC_LINES = [
  { key: "gc_minor_rate", name: "minor", color: "#94e2d5" },
  { key: "gc_major_rate", name: "major (large heap)", color: "#f9e2af" },
  { key: "gc_long_rate",  name: "long (>200ms)",     color: "#f38ba8" },
];

function GcChart({ data }) {
  const [hidden, setHidden] = useState({});
  const toggle = (e) => {
    const key = GC_LINES.find((l) => l.name === e.value)?.key;
    if (key) setHidden((h) => ({ ...h, [key]: !h[key] }));
  };
  return (
    <Panel title="GC Events /s (VM-wide minor · large-heap · long >200ms)">
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
          <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
          <YAxis tick={AXIS} width={65} />
          <Tooltip contentStyle={TOOLTIP} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086", cursor: "pointer" }} onClick={toggle} />
          {GC_LINES.map(({ key, name, color }) => (
            <Line key={key} type="monotone" dataKey={key} stroke={color} strokeWidth={2}
              dot={false} name={name} hide={!!hidden[key]} isAnimationActive={false} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </Panel>
  );
}

export default function S3Dashboard({ data = [], current = {}, producer_paused = false, mode = "none" }) {
  const pending = current.ets_pending ?? 0;
  const processing = current.ets_processing ?? 0;
  const sqsReady = current.sqs_visible ?? 0;
  const sqsInflight = current.sqs_inflight ?? 0;
  const writeRate = current.write_rate ?? 0;
  const writtenTotal = current.written_total ?? 0;
  const readRate = current.read_rate ?? 0;
  const readTotal = current.read_total ?? 0;
  const etsMb = current.ets_mb ?? 0;
  const procMb = current.proc_mb ?? 0;
  const chProcMb = current.ch_proc_mb ?? 0;
  const bqProcMb = current.bq_proc_mb ?? 0;
  const gcMinorRate = current.gc_minor_rate ?? 0;
  const gcMajorRate = current.gc_major_rate ?? 0;
  const gcLongRate = current.gc_long_rate ?? 0;
  const gcReclaimedMb = current.gc_reclaimed_mb ?? 0;
  const chBatchRate = current.ch_batch_rate ?? 0;
  const chTotal = current.ch_total ?? 0;
  const bqBatchRate = current.bq_batch_rate ?? 0;
  const bqTotal = current.bq_total ?? 0;
  const totalMb = current.total_mb ?? 0;

  return (
    <div style={{ background: "#181825", minHeight: "100vh", padding: "24px", fontFamily: "monospace" }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 24 }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <h1 style={{ color: "#cdd6f4", fontSize: 22, fontWeight: 700, margin: 0 }}>
              S3 Spool Dashboard
            </h1>
            <span
              style={{
                background: MODE_COLORS[mode]?.bg ?? "#313244",
                color: MODE_COLORS[mode]?.text ?? "#6c7086",
                fontSize: 11,
                fontWeight: 700,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                padding: "3px 10px",
                borderRadius: "99px",
              }}
            >
              {mode}
            </span>
          </div>
          <div style={{ color: "#6c7086", fontSize: 12, marginTop: 4 }}>
            Real-time producer · consumer · queue metrics
          </div>
        </div>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            background: "#1e1e2e",
            border: "1px solid #313244",
            borderRadius: "8px",
            padding: "8px 16px",
          }}
        >
          <div
            style={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: producer_paused ? "#f38ba8" : "#a6e3a1",
              boxShadow: producer_paused ? "0 0 6px #f38ba8" : "0 0 6px #a6e3a1",
            }}
          />
          <span style={{ color: "#cdd6f4", fontSize: 13 }}>
            Producer: {producer_paused ? "Paused" : "Running"}
          </span>
        </div>
      </div>

      {/* Stat rows */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 12 }}>
        <StatCard label="ETS Pending" value={pending} color="#f38ba8" />
        <StatCard label="ETS Processing" value={processing} color="#fab387" />
        <StatCard label="SQS Ready" value={sqsReady} color="#89b4fa" />
        <StatCard label="SQS In-flight" value={sqsInflight} color="#cba6f7" />
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 12 }}>
        <StatCard label="Written /s" value={writeRate} color="#a6e3a1" unit="/s" />
        <StatCard label="Total Written" value={writtenTotal} color="#a6e3a1" />
        <StatCard label="Read /s" value={readRate} color="#94e2d5" unit="/s" />
        <StatCard label="Total Read" value={readTotal} color="#94e2d5" />
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12, marginBottom: 20 }}>
        <StatCard label="Process Memory" value={procMb} color="#cba6f7" unit="MB" />
        <StatCard label="Total Memory" value={totalMb} color="#89dceb" unit="MB" />
      </div>

      {/* Charts grid */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <Panel title="ETS Queue Depth">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} />
              <Tooltip contentStyle={TOOLTIP} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
              <Area
                type="monotone"
                dataKey="ets_pending"
                stackId="1"
                stroke="#f38ba8"
                fill="#f38ba8"
                fillOpacity={0.35}
                name="pending"
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="ets_processing"
                stackId="1"
                stroke="#fab387"
                fill="#fab387"
                fillOpacity={0.35}
                name="processing"
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="SQS Queue Depth">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} />
              <Tooltip contentStyle={TOOLTIP} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
              <Area
                type="monotone"
                dataKey="sqs_visible"
                stackId="1"
                stroke="#89b4fa"
                fill="#89b4fa"
                fillOpacity={0.35}
                name="ready"
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="sqs_inflight"
                stackId="1"
                stroke="#cba6f7"
                fill="#cba6f7"
                fillOpacity={0.35}
                name="in-flight"
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Throughput (events/s written to S3)">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} />
              <Tooltip contentStyle={TOOLTIP} />
              <Line
                type="monotone"
                dataKey="throughput"
                stroke="#a6e3a1"
                strokeWidth={2}
                dot={false}
                name="events/s"
                isAnimationActive={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Memory Usage">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} unit="MB" />
              <Tooltip contentStyle={TOOLTIP} formatter={(v) => `${v} MB`} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
              <Area
                type="monotone"
                dataKey="total_mb"
                stroke="#89dceb"
                fill="#89dceb"
                fillOpacity={0.15}
                name="Total"
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="proc_mb"
                stroke="#cba6f7"
                fill="#cba6f7"
                fillOpacity={0.3}
                name="Processes"
                isAnimationActive={false}
              />
              <Area
                type="monotone"
                dataKey="ets_mb"
                stroke="#f9e2af"
                fill="#f9e2af"
                fillOpacity={0.3}
                name="ETS"
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        </Panel>
      </div>

      {/* CPU section */}
      <div style={{ marginTop: 32 }}>
        <div style={{ color: "#6c7086", fontSize: 11, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 12 }}>
          CPU
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12, marginBottom: 16 }}>
          <StatCard label="OS CPU" value={current.os_cpu ?? 0} color="#89b4fa" unit="%" />
          <StatCard label="BEAM Schedulers" value={current.scheduler_pct ?? 0} color="#fab387" unit="%" />
        </div>
        <Panel title="CPU Utilization">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} unit="%" domain={[0, 100]} />
              <Tooltip contentStyle={TOOLTIP} formatter={(v) => `${v}%`} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
              <Line type="monotone" dataKey="os_cpu" stroke="#89b4fa" strokeWidth={2} dot={false} name="OS CPU" isAnimationActive={false} />
              <Line type="monotone" dataKey="scheduler_pct" stroke="#fab387" strokeWidth={2} dot={false} name="BEAM Schedulers" isAnimationActive={false} />
            </LineChart>
          </ResponsiveContainer>
        </Panel>
      </div>

      {/* ClickHouse / BigQuery section */}
      <div style={{ marginTop: 32 }}>
        <div style={{ color: "#6c7086", fontSize: 11, fontWeight: 600, letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 12 }}>
          Pipeline
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, marginBottom: 16 }}>
          <StatCard label="CH Process Memory" value={chProcMb} color="#a6e3a1" unit="MB" />
          <StatCard label="BQ Process Memory" value={bqProcMb} color="#cba6f7" unit="MB" />
          <StatCard label="Minor GC /s" value={gcMinorRate} color="#94e2d5" unit="/s" />
          <StatCard label="Major GC /s" value={gcMajorRate} color="#f9e2af" unit="/s" />
          <StatCard label="Long GC /s" value={gcLongRate} color="#f38ba8" unit="/s" />
        </div>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          <Panel title="Pipeline Memory + GC Reclaimed">
            <ResponsiveContainer width="100%" height={200}>
              <ComposedChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
                <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
                <YAxis tick={AXIS} width={65} unit="MB" />
                <Tooltip contentStyle={TOOLTIP} formatter={(v) => `${v} MB`} />
                <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
                <Area type="monotone" dataKey="ch_proc_mb" stroke="#a6e3a1" fill="#a6e3a1" fillOpacity={0.3} name="ClickHouse" isAnimationActive={false} />
                <Area type="monotone" dataKey="bq_proc_mb" stroke="#cba6f7" fill="#cba6f7" fillOpacity={0.3} name="BigQuery" isAnimationActive={false} />
                <Line type="monotone" dataKey="gc_reclaimed_mb" stroke="#f9e2af" strokeWidth={2} dot={false} name="GC Reclaimed" isAnimationActive={false} />
              </ComposedChart>
            </ResponsiveContainer>
          </Panel>
          <GcChart data={data} />
        </div>
        <div style={{ marginTop: 16 }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 12 }}>
            <StatCard label="CH Total Acked" value={chTotal.toLocaleString()} color="#a6e3a1" />
            <StatCard label="BQ Total Acked" value={bqTotal.toLocaleString()} color="#cba6f7" />
            <StatCard label="CH events/s" value={chBatchRate} color="#a6e3a1" unit="/s" />
            <StatCard label="BQ events/s" value={bqBatchRate} color="#cba6f7" unit="/s" />
          </div>
          <Panel title="Pipeline Throughput (events/s)">
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
                <XAxis dataKey="t" tick={AXIS} interval="preserveStartEnd" />
                <YAxis tick={AXIS} width={65} unit="/s" />
                <Tooltip contentStyle={TOOLTIP} />
                <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
                <Line type="monotone" dataKey="ch_batch_rate" stroke="#a6e3a1" strokeWidth={2} dot={false} name="ClickHouse" isAnimationActive={false} />
                <Line type="monotone" dataKey="bq_batch_rate" stroke="#cba6f7" strokeWidth={2} dot={false} name="BigQuery" isAnimationActive={false} />
              </LineChart>
            </ResponsiveContainer>
          </Panel>
        </div>
      </div>
    </div>
  );
}
