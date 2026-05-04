import React, { useRef, useState } from "react";
import {
  AreaChart,
  Area,
  LineChart,
  Line,
  ComposedChart,
  Brush,
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
const SNAP_DASH = "5 4";

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

function ToolbarButton({ onClick, children, active = false, disabled = false, color = "#89b4fa" }) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        padding: "8px 16px",
        borderRadius: "6px",
        border: "1px solid #313244",
        cursor: disabled ? "not-allowed" : "pointer",
        fontFamily: "monospace",
        fontSize: 12,
        fontWeight: 600,
        background: active ? color : "#1e1e2e",
        color: active ? "#1e1e2e" : disabled ? "#45475a" : "#cdd6f4",
        opacity: disabled ? 0.6 : 1,
      }}
    >
      {children}
    </button>
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

// The key metrics tracked in the Window Stats comparison table.
const WINDOW_METRICS = [
  { key: "write_rate", label: "Written /s", unit: "/s" },
  { key: "read_rate", label: "Read /s", unit: "/s" },
  { key: "ch_batch_rate", label: "CH events/s", unit: "/s" },
  { key: "bq_batch_rate", label: "BQ events/s", unit: "/s" },
  { key: "ets_mb", label: "ETS Memory", unit: "MB" },
  { key: "proc_mb", label: "Process Memory", unit: "MB" },
  { key: "total_mb", label: "Total Memory", unit: "MB" },
  { key: "ch_proc_mb", label: "CH Process Memory", unit: "MB" },
  { key: "bq_proc_mb", label: "BQ Process Memory", unit: "MB" },
  { key: "os_cpu", label: "OS CPU", unit: "%" },
  { key: "scheduler_pct", label: "BEAM Schedulers", unit: "%" },
  { key: "gc_minor_rate", label: "Minor GC /s", unit: "/s" },
  { key: "gc_reclaimed_mb", label: "GC Reclaimed", unit: "MB" },
];

function fmt(v) {
  if (typeof v !== "number" || Number.isNaN(v)) return "-";
  return Number.isInteger(v) ? v.toLocaleString() : v.toFixed(2);
}

function aggregate(rows, key) {
  const vals = rows.map((r) => r[key]).filter((v) => typeof v === "number" && !Number.isNaN(v));
  if (!vals.length) return { min: 0, max: 0, avg: 0 };
  return {
    min: Math.min(...vals),
    max: Math.max(...vals),
    avg: vals.reduce((a, b) => a + b, 0) / vals.length,
  };
}

// Resolves the active window as [start, end] indices into `chartData`. Start/Stop
// measuring takes precedence over a brush drag-selection; with neither active,
// the window auto-follows the most recent `defaultViewPoints` samples so the
// charts stay readable instead of cramming the whole retained buffer in.
function computeWindowBounds({ dataLength, measureStartIdx, measuring, measureEndIdx, brushRange, defaultViewPoints }) {
  if (dataLength === 0) return { start: 0, end: -1 };

  if (measureStartIdx != null) {
    const end = measuring ? dataLength - 1 : measureEndIdx ?? dataLength - 1;
    return { start: Math.min(measureStartIdx, dataLength - 1), end };
  }

  if (brushRange) {
    return { start: brushRange.startIndex, end: Math.min(brushRange.endIndex, dataLength - 1) };
  }

  return { start: Math.max(0, dataLength - defaultViewPoints), end: dataLength - 1 };
}

function rowLabel(row) {
  if (!row) return "?";
  return row.t ?? `+${row.elapsed_s}s`;
}

// Elapsed seconds since the first row — lets two separate runs (which don't
// share a wall-clock start) be plotted on the same relative time axis.
function withElapsed(rows) {
  if (!rows.length) return [];
  const firstTs = rows[0].ts ?? 0;
  return rows.map((r) => ({ ...r, elapsed_s: Math.round(((r.ts ?? firstTs) - firstTs) / 1000) }));
}

// Left-joins the live series and a loaded snapshot's series by elapsed_s,
// prefixing the snapshot's fields with `snap_` so both can be plotted as
// separate lines/areas on the same existing charts.
function mergeByElapsed(liveRows, snapRows) {
  const byElapsed = new Map();

  for (const row of withElapsed(liveRows)) {
    byElapsed.set(row.elapsed_s, { ...row });
  }

  for (const row of withElapsed(snapRows)) {
    const prefixed = { elapsed_s: row.elapsed_s };
    for (const [k, v] of Object.entries(row)) {
      if (k !== "elapsed_s" && k !== "ts" && k !== "t") prefixed[`snap_${k}`] = v;
    }
    byElapsed.set(row.elapsed_s, { ...(byElapsed.get(row.elapsed_s) ?? { elapsed_s: row.elapsed_s }), ...prefixed });
  }

  return Array.from(byElapsed.values()).sort((a, b) => a.elapsed_s - b.elapsed_s);
}

function downloadSnapshot(data, mode) {
  const payload = { savedAt: Date.now(), mode, data };
  const blob = new Blob([JSON.stringify(payload)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");

  const a = document.createElement("a");
  a.href = url;
  a.download = `logflare-spool-snapshot-${stamp}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// Stats are computed over the same windowed slice the charts render, so the
// table always matches what's on screen — including the snapshot's stats,
// which are scoped to the same window (via its `snap_`-prefixed fields)
// rather than always summarizing its entire run.
function WindowStatsPanel({ visibleChartData, hasSnapshot }) {
  const span =
    visibleChartData.length > 0
      ? `${rowLabel(visibleChartData[0])} → ${rowLabel(visibleChartData[visibleChartData.length - 1])} (${visibleChartData.length} samples)`
      : "no data";

  return (
    <Panel title={`Window Stats — ${span}`}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12, color: "#cdd6f4" }}>
        <thead>
          <tr style={{ color: "#6c7086", textTransform: "uppercase", fontSize: 10, letterSpacing: "0.05em" }}>
            <th style={{ textAlign: "left", padding: "4px 8px" }}>Metric</th>
            <th style={{ textAlign: "right", padding: "4px 8px" }}>Min</th>
            <th style={{ textAlign: "right", padding: "4px 8px" }}>Avg</th>
            <th style={{ textAlign: "right", padding: "4px 8px" }}>Max</th>
            {hasSnapshot && (
              <>
                <th style={{ textAlign: "right", padding: "4px 8px", color: "#fab387" }}>Snap Min</th>
                <th style={{ textAlign: "right", padding: "4px 8px", color: "#fab387" }}>Snap Avg</th>
                <th style={{ textAlign: "right", padding: "4px 8px", color: "#fab387" }}>Snap Max</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {WINDOW_METRICS.map(({ key, label, unit }) => {
            const live = aggregate(visibleChartData, key);
            const snap = hasSnapshot ? aggregate(visibleChartData, `snap_${key}`) : null;
            return (
              <tr key={key} style={{ borderTop: "1px solid #313244" }}>
                <td style={{ padding: "4px 8px" }}>{label}</td>
                <td style={{ textAlign: "right", padding: "4px 8px" }}>{fmt(live.min)} {unit}</td>
                <td style={{ textAlign: "right", padding: "4px 8px" }}>{fmt(live.avg)} {unit}</td>
                <td style={{ textAlign: "right", padding: "4px 8px" }}>{fmt(live.max)} {unit}</td>
                {snap && (
                  <>
                    <td style={{ textAlign: "right", padding: "4px 8px", color: "#fab387" }}>{fmt(snap.min)} {unit}</td>
                    <td style={{ textAlign: "right", padding: "4px 8px", color: "#fab387" }}>{fmt(snap.avg)} {unit}</td>
                    <td style={{ textAlign: "right", padding: "4px 8px", color: "#fab387" }}>{fmt(snap.max)} {unit}</td>
                  </>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </Panel>
  );
}

function GcChart({ data, xKey, snapshot }) {
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
          <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
          <YAxis tick={AXIS} width={65} />
          <Tooltip contentStyle={TOOLTIP} />
          <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086", cursor: "pointer" }} onClick={toggle} />
          {GC_LINES.map(({ key, name, color }) => (
            <Line key={key} type="monotone" dataKey={key} stroke={color} strokeWidth={2}
              dot={false} name={name} hide={!!hidden[key]} isAnimationActive={false} />
          ))}
          {snapshot &&
            GC_LINES.map(({ key, name, color }) => (
              <Line
                key={`snap_${key}`}
                type="monotone"
                dataKey={`snap_${key}`}
                stroke={color}
                strokeWidth={1.5}
                strokeDasharray={SNAP_DASH}
                strokeOpacity={0.6}
                dot={false}
                name={`${name} (snapshot)`}
                hide={!!hidden[key]}
                isAnimationActive={false}
              />
            ))}
        </LineChart>
      </ResponsiveContainer>
    </Panel>
  );
}

const DEFAULT_VIEW_POINTS = 60; // ~1 minute at the 1s tick cadence

export default function DevDashboard({ data = [], current = {}, producer_paused = false, mode = "none" }) {
  const [brushRange, setBrushRange] = useState(null);
  const [measureStartIdx, setMeasureStartIdx] = useState(null);
  const [measuring, setMeasuring] = useState(false);
  const [measureEndIdx, setMeasureEndIdx] = useState(null);
  const [snapshot, setSnapshot] = useState(null);
  const [frozen, setFrozen] = useState(false);
  const [frozenData, setFrozenData] = useState(null);
  const fileInputRef = useRef(null);

  const hasSnapshot = !!snapshot;
  // Frozen stops the charts from redrawing/scrolling while comparing a
  // loaded snapshot — `liveBase` pins to a captured copy instead of `data`.
  const liveBase = frozen && frozenData ? frozenData : data;
  const chartData = hasSnapshot ? mergeByElapsed(liveBase, snapshot.data) : liveBase;
  const xKey = hasSnapshot ? "elapsed_s" : "t";

  const { start, end } = computeWindowBounds({
    dataLength: chartData.length,
    measureStartIdx,
    measuring,
    measureEndIdx,
    brushRange,
    defaultViewPoints: DEFAULT_VIEW_POINTS,
  });
  const visibleChartData = end >= start ? chartData.slice(start, end + 1) : [];

  const handleStart = () => {
    setBrushRange(null);
    setMeasureEndIdx(null);
    setMeasureStartIdx(chartData.length);
    setMeasuring(true);
  };

  const handleStop = () => {
    setMeasureEndIdx(chartData.length - 1);
    setMeasuring(false);
  };

  const handleClearWindow = () => {
    setBrushRange(null);
    setMeasureStartIdx(null);
    setMeasuring(false);
    setMeasureEndIdx(null);
  };

  const handleBrushChange = (range) => {
    if (measureStartIdx == null && range && typeof range.startIndex === "number") {
      setBrushRange(range);
    }
  };

  const handleToggleFreeze = () => {
    if (frozen) {
      setFrozen(false);
      setFrozenData(null);
    } else {
      setFrozenData(data);
      setFrozen(true);
    }
  };

  const handleSaveSnapshot = () => downloadSnapshot(data, mode);

  const handleLoadSnapshot = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = () => {
      try {
        const parsed = JSON.parse(reader.result);
        if (Array.isArray(parsed?.data)) {
          setSnapshot(parsed);
          // Freeze automatically — comparing against a moving live chart is
          // hard to read, so pin the live side the moment a snapshot loads.
          setFrozenData(data);
          setFrozen(true);
        }
      } catch (err) {
        console.error("Failed to parse snapshot file", err);
      }
    };
    reader.readAsText(file);
    e.target.value = "";
  };

  const handleClearSnapshot = () => setSnapshot(null);

  const pending = current.ets_pending ?? 0;
  const processing = current.ets_processing ?? 0;
  const sqsReady = current.sqs_visible ?? 0;
  const sqsInflight = current.sqs_inflight ?? 0;
  const writeRate = current.write_rate ?? 0;
  const writtenTotal = current.written_total ?? 0;
  const readRate = current.read_rate ?? 0;
  const readTotal = current.read_total ?? 0;
  const procMb = current.proc_mb ?? 0;
  const chProcMb = current.ch_proc_mb ?? 0;
  const bqProcMb = current.bq_proc_mb ?? 0;
  const gcMinorRate = current.gc_minor_rate ?? 0;
  const gcMajorRate = current.gc_major_rate ?? 0;
  const gcLongRate = current.gc_long_rate ?? 0;
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
              Spool Dashboard
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

      {/* Window / snapshot toolbar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          marginBottom: 16,
          padding: "10px 16px",
          background: "#1e1e2e",
          border: "1px solid #313244",
          borderRadius: "8px",
          flexWrap: "wrap",
        }}
      >
        {measuring && (
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <div
              style={{
                width: 8,
                height: 8,
                borderRadius: "50%",
                background: "#f38ba8",
                boxShadow: "0 0 6px #f38ba8",
              }}
            />
            <span style={{ color: "#f38ba8", fontSize: 12 }}>Measuring…</span>
          </div>
        )}
        <ToolbarButton onClick={handleStart} disabled={measuring} active={measuring} color="#a6e3a1">
          ▶ Start
        </ToolbarButton>
        <ToolbarButton onClick={handleStop} disabled={!measuring} color="#f38ba8">
          ■ Stop
        </ToolbarButton>
        <ToolbarButton onClick={handleClearWindow}>Clear window</ToolbarButton>

        <div style={{ width: 1, height: 20, background: "#313244", margin: "0 4px" }} />

        <ToolbarButton onClick={handleToggleFreeze} active={frozen} color="#89dceb">
          {frozen ? "▶ Resume live" : "❄ Freeze"}
        </ToolbarButton>

        <div style={{ width: 1, height: 20, background: "#313244", margin: "0 4px" }} />

        <ToolbarButton onClick={handleSaveSnapshot} color="#89b4fa">
          ⭳ Save Snapshot
        </ToolbarButton>
        <ToolbarButton onClick={() => fileInputRef.current?.click()} color="#89b4fa">
          ⭱ Load Snapshot
        </ToolbarButton>
        <input
          ref={fileInputRef}
          type="file"
          accept="application/json"
          onChange={handleLoadSnapshot}
          style={{ display: "none" }}
        />
        {hasSnapshot && (
          <>
            <span style={{ color: "#fab387", fontSize: 12 }}>
              Comparing against snapshot saved {new Date(snapshot.savedAt).toLocaleString()}
            </span>
            <ToolbarButton onClick={handleClearSnapshot}>Clear snapshot</ToolbarButton>
          </>
        )}
      </div>

      {/* Timeline / brush selector — shows the full retained buffer; drag the
          handles to scroll/zoom the charts below through it. Defaults to the
          most recent ~1 minute so labels don't overlap. */}
      <div style={{ marginBottom: 16 }}>
        <Panel title={`Timeline — full buffer (${chartData.length} samples) · drag to scroll/zoom`}>
          <ResponsiveContainer width="100%" height={90}>
            <LineChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
              <Line type="monotone" dataKey="write_rate" stroke="#a6e3a1" strokeWidth={1.5} dot={false} isAnimationActive={false} />
              {hasSnapshot && (
                <Line type="monotone" dataKey="snap_write_rate" stroke="#a6e3a1" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} isAnimationActive={false} />
              )}
              <Brush
                dataKey={xKey}
                height={24}
                travellerWidth={8}
                stroke="#89b4fa"
                fill="#1e1e2e"
                onChange={handleBrushChange}
                startIndex={start}
                endIndex={end}
              />
            </LineChart>
          </ResponsiveContainer>
        </Panel>
      </div>

      {/* Window stats */}
      <div style={{ marginBottom: 20 }}>
        <WindowStatsPanel visibleChartData={visibleChartData} hasSnapshot={hasSnapshot} />
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
            <AreaChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
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
              {hasSnapshot && (
                <>
                  <Line type="monotone" dataKey="snap_ets_pending" stroke="#f38ba8" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="pending (snapshot)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="snap_ets_processing" stroke="#fab387" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="processing (snapshot)" isAnimationActive={false} />
                </>
              )}
            </AreaChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="SQS Queue Depth">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
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
              {hasSnapshot && (
                <>
                  <Line type="monotone" dataKey="snap_sqs_visible" stroke="#89b4fa" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="ready (snapshot)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="snap_sqs_inflight" stroke="#cba6f7" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="in-flight (snapshot)" isAnimationActive={false} />
                </>
              )}
            </AreaChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Throughput (events/s written to spool)">
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} />
              <Tooltip contentStyle={TOOLTIP} />
              <Line
                type="monotone"
                dataKey="write_rate"
                stroke="#a6e3a1"
                strokeWidth={2}
                dot={false}
                name="events/s"
                isAnimationActive={false}
              />
              {hasSnapshot && (
                <Line type="monotone" dataKey="snap_write_rate" stroke="#a6e3a1" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="events/s (snapshot)" isAnimationActive={false} />
              )}
            </LineChart>
          </ResponsiveContainer>
        </Panel>

        <Panel title="Memory Usage">
          <ResponsiveContainer width="100%" height={200}>
            <AreaChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
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
              {hasSnapshot && (
                <>
                  <Line type="monotone" dataKey="snap_total_mb" stroke="#89dceb" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="Total (snapshot)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="snap_proc_mb" stroke="#cba6f7" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="Processes (snapshot)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="snap_ets_mb" stroke="#f9e2af" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="ETS (snapshot)" isAnimationActive={false} />
                </>
              )}
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
            <LineChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
              <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
              <YAxis tick={AXIS} width={65} unit="%" domain={[0, 100]} />
              <Tooltip contentStyle={TOOLTIP} formatter={(v) => `${v}%`} />
              <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
              <Line type="monotone" dataKey="os_cpu" stroke="#89b4fa" strokeWidth={2} dot={false} name="OS CPU" isAnimationActive={false} />
              <Line type="monotone" dataKey="scheduler_pct" stroke="#fab387" strokeWidth={2} dot={false} name="BEAM Schedulers" isAnimationActive={false} />
              {hasSnapshot && (
                <>
                  <Line type="monotone" dataKey="snap_os_cpu" stroke="#89b4fa" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="OS CPU (snapshot)" isAnimationActive={false} />
                  <Line type="monotone" dataKey="snap_scheduler_pct" stroke="#fab387" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="BEAM Schedulers (snapshot)" isAnimationActive={false} />
                </>
              )}
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
              <ComposedChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
                <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
                <YAxis tick={AXIS} width={65} unit="MB" />
                <Tooltip contentStyle={TOOLTIP} formatter={(v) => `${v} MB`} />
                <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
                <Area type="monotone" dataKey="ch_proc_mb" stroke="#a6e3a1" fill="#a6e3a1" fillOpacity={0.3} name="ClickHouse" isAnimationActive={false} />
                <Area type="monotone" dataKey="bq_proc_mb" stroke="#cba6f7" fill="#cba6f7" fillOpacity={0.3} name="BigQuery" isAnimationActive={false} />
                <Line type="monotone" dataKey="gc_reclaimed_mb" stroke="#f9e2af" strokeWidth={2} dot={false} name="GC Reclaimed" isAnimationActive={false} />
                {hasSnapshot && (
                  <>
                    <Line type="monotone" dataKey="snap_ch_proc_mb" stroke="#a6e3a1" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="ClickHouse (snapshot)" isAnimationActive={false} />
                    <Line type="monotone" dataKey="snap_bq_proc_mb" stroke="#cba6f7" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="BigQuery (snapshot)" isAnimationActive={false} />
                    <Line type="monotone" dataKey="snap_gc_reclaimed_mb" stroke="#f9e2af" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="GC Reclaimed (snapshot)" isAnimationActive={false} />
                  </>
                )}
              </ComposedChart>
            </ResponsiveContainer>
          </Panel>
          <GcChart data={visibleChartData} xKey={xKey} snapshot={hasSnapshot} />
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
              <LineChart data={visibleChartData} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={GRID} />
                <XAxis dataKey={xKey} tick={AXIS} interval="preserveStartEnd" />
                <YAxis tick={AXIS} width={65} unit="/s" />
                <Tooltip contentStyle={TOOLTIP} />
                <Legend wrapperStyle={{ fontSize: 11, color: "#6c7086" }} />
                <Line type="monotone" dataKey="ch_batch_rate" stroke="#a6e3a1" strokeWidth={2} dot={false} name="ClickHouse" isAnimationActive={false} />
                <Line type="monotone" dataKey="bq_batch_rate" stroke="#cba6f7" strokeWidth={2} dot={false} name="BigQuery" isAnimationActive={false} />
                {hasSnapshot && (
                  <>
                    <Line type="monotone" dataKey="snap_ch_batch_rate" stroke="#a6e3a1" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="ClickHouse (snapshot)" isAnimationActive={false} />
                    <Line type="monotone" dataKey="snap_bq_batch_rate" stroke="#cba6f7" strokeDasharray={SNAP_DASH} strokeOpacity={0.6} dot={false} name="BigQuery (snapshot)" isAnimationActive={false} />
                  </>
                )}
              </LineChart>
            </ResponsiveContainer>
          </Panel>
        </div>
      </div>
    </div>
  );
}
