defmodule Logflare.Sources.Source.RateSampler do
  @moduledoc """
  Tracks local per-key throughput (events/sec) and decides whether to
  sample a rate-gated action against it. Keys are arbitrary terms, not
  just source tokens — a caller with more than one independent rate to
  track for the same source (e.g. dispatch rate vs. a backend's own
  processing rate) can key each separately.

  `bump/2` records a batch's size against a key's rolling window -- call
  it once per batch, not per event. `sample?/1` is a read-only
  probability check against the current rate and can be called as many
  times as needed without affecting the counter. `rate/1` returns the
  current numeric rate directly, for callers that want their own
  threshold instead of probabilistic sampling.

  A key's first `bump/2` seeds a provisional rate from that batch's own
  size, rather than starting at a rate of 0 (which would sample
  everything) until a full window has elapsed.
  """

  use GenServer

  import Logflare.Utils.Guards, only: [is_pos_integer: 1]

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
  Records `count` events for `key` in the current rolling window.
  Call this once per batch — see the moduledoc.
  """
  @spec bump(term(), pos_integer()) :: :ok
  def bump(key, count) when is_pos_integer(count) do
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@table, key) do
      [{^key, window_start, _count, _last_rate}] when now - window_start < @window_ms ->
        :ets.update_counter(@table, key, {3, count})

      [{^key, window_start, existing_count, _last_rate}] ->
        new_rate = existing_count * 1000 / max(now - window_start, 1)
        :ets.insert(@table, {key, now, count, new_rate})

      [] ->
        provisional_rate = count * 1000 / @window_ms
        :ets.insert(@table, {key, now, count, provisional_rate})
    end

    :ok
  end

  @doc "Whether to sample the next rate-gated action for `key`."
  @spec sample?(term()) :: boolean()
  def sample?(key) do
    :rand.uniform() <= probability(key)
  end

  @doc "Current estimated rate (events/sec) for `key`, or 0.0 if never bumped."
  @spec rate(term()) :: float()
  def rate(key) do
    case :ets.lookup(@table, key) do
      [{^key, _window_start, _count, rate}] -> rate
      [] -> 0.0
    end
  end

  defp probability(key) do
    case rate(key) do
      rate when rate > 0 -> min(1.0, max(0.00001, 1.0 / rate))
      _ -> 1.0
    end
  end
end
