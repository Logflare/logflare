defmodule Logflare.Sources.Source.BigQuery.SchemaUpdateSampler do
  @moduledoc """
  Decides whether to sample a schema-update check for a source's event,
  based on this node's own recent local throughput for that source.
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

  # probability = 1.0 / rate, with safety bounds. A never-seen or idle
  # source has no rate yet, so it samples every event until one is computed.
  defp probability(source_token) do
    case bump_and_get_rate(source_token) do
      rate when rate > 0 -> min(1.0, max(0.00001, 1.0 / rate))
      _ -> 1.0
    end
  end

  # A rolling per-source rate: every call bumps a count; once @window_ms
  # elapses, the completed window's rate becomes the value returned until
  # the next window completes.
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
