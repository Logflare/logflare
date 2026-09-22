defmodule Logflare.Sources.Source.RateSampler do
  @moduledoc """
  Decides whether to sample a per-event, rate-gated action for a source (a
  BigQuery schema-update check, a dashboard live-tail broadcast), based on this
  node's own recent local throughput for that source.

  Counting and sampling are deliberately split into two separate calls:

    * `bump/2` records a batch's size against a source's rolling window. Call
      this exactly once per dispatched batch — see `Backends.dispatch/2` — not
      once per event, so the (batch-sized, cheap either way) count update
      happens once regardless of how many downstream consumers later check
      `sample?/1` for that same batch.
    * `sample?/1` is a read-only probability check. Call it as many times as
      needed (once per event, from as many independent call sites as needed)
      without touching the counter — every caller reads the same underlying
      rate, updated in the one place `bump/2` is called.

  This unifies what used to be two separately-drifting signals: a BigQuery-only
  `SchemaUpdateSampler` (which bumped its own counter once per event, inside the
  BigQuery pipeline specifically) and the dashboard broadcast's `source.metrics
  .avg` (sourced from cluster-wide `PubSubRates`, refreshed only once per
  inbound request — before that request's own events were counted, so a large
  first burst on an otherwise-quiet source was invisible to it until the
  *next* request). Being purely local ETS state written by whichever process
  calls `bump/2` — no cluster/PubSub round-trip — also means this keeps working
  correctly if the code path that decides whether to sample moves to run
  somewhere other than wherever the original request was handled (e.g. a
  separate consumer process, once spool producer/consumer are split).

  A never-seen source's first `bump/2` seeds an immediate provisional rate from
  that first batch's own size (`count * 1000 / window_ms`), rather than leaving
  the rate at 0 (which would mean "sample everything") until a full window has
  elapsed — a large first burst is throttled starting from the batch that
  introduces it, not just from the window after.
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

  # probability = 1.0 / rate, with safety bounds. A never-bumped source has no
  # rate yet, so it samples every event until bump/2 has been called for it.
  defp probability(source_token) do
    case :ets.lookup(@table, source_token) do
      [{^source_token, _window_start, _count, rate}] when rate > 0 ->
        min(1.0, max(0.00001, 1.0 / rate))

      _ ->
        1.0
    end
  end
end
