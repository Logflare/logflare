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

  @doc "Sample a schema-update check, returning the rate mode or `:skip`."
  @spec sample_mode(atom()) :: :normal | :zero_rate | :floor | :skip
  def sample_mode(source_token) do
    {probability, mode} = sampling_probability(bump_and_get_rate(source_token))
    if :rand.uniform() <= probability, do: mode, else: :skip
  end

  # A never-seen or idle source has no rate yet, so it samples every event
  # until one is computed. Keep the rate mode for low-cardinality telemetry.
  @doc false
  @spec sampling_probability(number()) :: {float(), :normal | :zero_rate | :floor}
  def sampling_probability(rate) do
    cond do
      rate <= 0 -> {1.0, :zero_rate}
      rate > 100_000 -> {0.00001, :floor}
      true -> {min(1.0, 1.0 / rate), :normal}
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
