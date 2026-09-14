defmodule Logflare.Sources.Source.BigQuery.SchemaUpdateSampler do
  @moduledoc """
  Decides whether to sample a schema-update check for a source's event,
  based on this node's own recent local throughput for that source —
  deliberately independent of `Logflare.Sources.Counters`/`PubSubRates`
  (billing, dashboard rate, and quota-enforcement all read those, and
  double-counting there would corrupt all three). This has its own,
  dedicated ETS table so it can never be confused with or accidentally
  feed any of them.

  Mirrors `Logflare.Sources.RateCounters`'s pattern of a `GenServer` that
  only exists to own a `:public` ETS table, initialized once, then never
  called via message-passing again.
  """

  use GenServer

  @table :schema_update_sampler
  @window_ms 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(state) do
    :ets.new(@table, [:public, :named_table, write_concurrency: true, read_concurrency: true])
    {:ok, state}
  end

  @doc "Whether to sample a schema-update check for `source_token`'s next event."
  @spec sample?(atom()) :: boolean()
  def sample?(source_token) do
    :rand.uniform() <= probability(source_token)
  end

  # probability = 1.0 / rate, with safety bounds — supports rates up to
  # 100K+/sec: at 100K/sec -> 0.00001 (samples ~1/sec). A never-seen or
  # currently-idle source has no rate yet, so it samples every event until
  # enough local calls land to compute one — same intent as the original
  # cluster-rate-based version, just fed by this node's real throughput.
  defp probability(source_token) do
    case bump_and_get_rate(source_token) do
      rate when rate > 0 -> min(1.0, max(0.00001, 1.0 / rate))
      _ -> 1.0
    end
  end

  # A rolling per-source rate: every call bumps a count; once @window_ms
  # elapses, the completed window's rate becomes the value returned (and
  # used) until the next window completes. Deliberately race-tolerant, not
  # race-free — concurrent calls can occasionally lose an increment or
  # reset a window slightly early, which only ever shifts the sampled rate
  # slightly for one window. It can never regress to always-sample, since
  # a cold source still resolves to `rate = 0` -> `probability = 1.0` for
  # its first window only, then converges down as real calls accumulate.
  defp bump_and_get_rate(source_token) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, source_token) do
      [{^source_token, window_start, _count, last_rate}] when now - window_start < @window_ms ->
        :ets.update_counter(@table, source_token, {3, 1})
        last_rate

      [{^source_token, window_start, count, _last_rate}] ->
        new_rate = count * 1000 / max(now - window_start, 1)
        :ets.insert(@table, {source_token, now, 1, new_rate})
        new_rate

      [] ->
        :ets.insert(@table, {source_token, now, 1, 0})
        0
    end
  end
end
