defmodule Logflare.LogEvent.DayBucket do
  @moduledoc """
  Day-bucket math and a cached value of the current UTC day bucket.

  A "day bucket" is `microseconds-since-epoch ÷ microseconds-per-day`. It's
  used on the ingest hot path to partition events by UTC day without
  carrying a full `DateTime` through the pipeline.

  `current/0` reads the cached "current bucket" from `:persistent_term` so
  per-event lookups avoid a syscall and `div/2`. The bucket value changes
  once per UTC day; this GenServer seeds the cached value at boot via
  `handle_continue/2` and refreshes shortly after midnight UTC. If the term
  is missing (e.g. early in boot before `handle_continue/2` runs, or in
  partially-started test environments), `current/0` falls back to an
  on-demand computation.
  """

  use GenServer

  @microseconds_per_day 86_400 * 1_000_000
  @pt_key {__MODULE__, :current}
  @refresh_buffer_ms 1_000

  @doc """
  Returns the current UTC day bucket, read from the `:persistent_term` cache.

  Falls back to an on-demand computation if the cache is missing (early
  boot or partially-started test environments).
  """
  @spec current() :: integer()
  def current do
    # intentionally using the 1-arity variant to avoid the comparison overhead
    # the negative is it will raise if the term is not set, hence the rescue
    :persistent_term.get(@pt_key)
  rescue
    ArgumentError -> compute()
  end

  @doc """
  Converts a microsecond Unix timestamp into its UTC day bucket.

  `div/2` truncates toward zero, so pre-epoch timestamps produce negative
  bucket values.

  ## Examples

      iex> Logflare.LogEvent.DayBucket.from_microseconds(0)
      0

      iex> Logflare.LogEvent.DayBucket.from_microseconds(86_400_000_000)
      1

      iex> Logflare.LogEvent.DayBucket.from_microseconds(-86_400_000_000)
      -1

  """
  @spec from_microseconds(integer()) :: integer()
  def from_microseconds(ts) when is_integer(ts) do
    div(ts, @microseconds_per_day)
  end

  @doc """
  Classifies a day bucket against the current UTC day bucket.

  Returns `:fresh` when the bucket is within one day of the current day
  bucket (a ±1-day window absorbs clock skew and timezone-edge events),
  and `:stale` otherwise.
  """
  @spec classify_freshness(integer()) :: :fresh | :stale
  def classify_freshness(bucket) when is_integer(bucket) do
    if abs(bucket - current()) <= 1, do: :fresh, else: :stale
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    {:ok, nil, {:continue, :refresh}}
  end

  @impl true
  def handle_continue(:refresh, state) do
    refresh()
    schedule_refresh()
    {:noreply, state}
  end

  @impl true
  def handle_info(:refresh, state) do
    refresh()
    schedule_refresh()
    {:noreply, state}
  end

  defp refresh do
    :persistent_term.put(@pt_key, compute())
  end

  defp compute do
    div(System.system_time(:microsecond), @microseconds_per_day)
  end

  defp schedule_refresh do
    now_us = System.system_time(:microsecond)
    next_day_us = (div(now_us, @microseconds_per_day) + 1) * @microseconds_per_day
    delay_ms = div(next_day_us - now_us, 1_000) + @refresh_buffer_ms
    Process.send_after(self(), :refresh, delay_ms)
  end
end
