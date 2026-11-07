defmodule Logflare.Backends.Spool.OverloadLog do
  @moduledoc """
  Rate-limited logging for spool appends shed because every candidate
  partition was backed up. Under overload every request is shed, so logging
  each one would flood the logs; this logs at most once per interval and
  reports how many appends were shed since the last line.
  """

  require Logger

  @default_interval_ms 10_000
  @last_logged_at 1
  @shed_count 2

  @spec record(keyword()) :: :ok
  def record(metadata \\ []) do
    state = state()
    :atomics.add(state, @shed_count, 1)
    now = System.system_time(:millisecond)
    last_logged_at = :atomics.get(state, @last_logged_at)

    if now - last_logged_at >= interval_ms() and
         :atomics.compare_exchange(state, @last_logged_at, last_logged_at, now) == :ok do
      shed = :atomics.exchange(state, @shed_count, 0)

      Logger.error(
        "backends: spool overloaded, shed #{shed} append(s) in the last #{interval_ms()}ms",
        metadata
      )
    end

    :ok
  end

  defp state do
    case :persistent_term.get(__MODULE__, nil) do
      nil ->
        state = :atomics.new(2, signed: true)
        :persistent_term.put(__MODULE__, state)
        state

      state ->
        state
    end
  end

  defp interval_ms,
    do:
      :logflare
      |> Application.get_env(:spool, [])
      |> Keyword.get(:overload_log_interval_ms, @default_interval_ms)
end
