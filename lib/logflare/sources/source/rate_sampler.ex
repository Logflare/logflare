defmodule Logflare.Sources.Source.RateSampler do
  @moduledoc """
  Decides whether to sample a per-event, rate-gated action for a source
  (e.g. a BigQuery schema-update check, a dashboard live-tail broadcast),
  based on this node's own recent local throughput for that source.

  `bump/2` records a batch's size against a source's rolling window --
  call it once per dispatched batch, not per event. `sample?/1` is a
  read-only probability check against the current rate and can be
  called as many times as needed without affecting the counter.

  A source's first `bump/2` seeds a provisional rate from that batch's
  own size, rather than starting at a rate of 0 (which would sample
  everything) until a full window has elapsed.
  """

  use GenServer

  @table :source_rate_sampler
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

  @doc """
  Records `count` events for `source_token` in the current rolling window.
  Call this once per dispatched batch — see the moduledoc.
  """
  @spec bump(atom(), pos_integer()) :: :ok
  def bump(source_token, count) when is_integer(count) and count > 0 do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, source_token) do
      [{^source_token, window_start, _count, _last_rate}] when now - window_start < @window_ms ->
        :ets.update_counter(@table, source_token, {3, count})

      [{^source_token, window_start, existing_count, _last_rate}] ->
        new_rate = existing_count * 1000 / max(now - window_start, 1)
        :ets.insert(@table, {source_token, now, count, new_rate})

      [] ->
        provisional_rate = count * 1000 / @window_ms
        :ets.insert(@table, {source_token, now, count, provisional_rate})
    end

    :ok
  end

  @doc "Whether to sample the next rate-gated action for `source_token`."
  @spec sample?(atom()) :: boolean()
  def sample?(source_token) do
    :rand.uniform() <= probability(source_token)
  end

  defp probability(source_token) do
    case :ets.lookup(@table, source_token) do
      [{^source_token, _window_start, _count, rate}] when rate > 0 ->
        min(1.0, max(0.00001, 1.0 / rate))

      _ ->
        1.0
    end
  end
end
