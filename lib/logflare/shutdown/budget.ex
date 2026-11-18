defmodule Logflare.Shutdown.Budget do
  @moduledoc """
  Calculates the latest end of each shutdown phase. A phase can finish early,
  but cannot borrow the reserved time of a later phase.
  """

  @stop_intake_ms 1_000
  @listener_stop_ms 1_000
  @writes_ms 1_000
  @sync_ms 10_000
  @cleanup_ms 10_000

  @type limit :: {cutoff :: integer(), max_ms :: non_neg_integer()}
  @type flush :: %{queues: limit(), sync: limit(), cleanup: limit()}
  @type t :: %{
          intake: limit(),
          withdrawal: integer(),
          listeners: limit(),
          requests: limit(),
          writes: limit(),
          flush: flush() | nil
        }

  @spec plan(integer(), keyword()) :: t()
  def plan(now, opts) do
    overall = Keyword.fetch!(opts, :deadline_ms)
    withdrawal_ms = Keyword.fetch!(opts, :withdrawal_ms)
    request_ms = Keyword.fetch!(opts, :request_timeout_ms)
    drain_ms = Keyword.fetch!(opts, :drain_timeout_ms)
    spool_buffer = Keyword.get(opts, :spool_buffer)

    {flush_start, flush} = flush_plan(now, overall, drain_ms, spool_buffer)
    withdrawal = min(now + withdrawal_ms, flush_start)
    writes_ms = min(@writes_ms, div(max(flush_start - withdrawal, 0), 2))
    requests_end = flush_start - writes_ms

    %{
      intake: {requests_end, @stop_intake_ms},
      withdrawal: withdrawal,
      listeners: {requests_end, @listener_stop_ms},
      requests: {requests_end, request_ms},
      writes: {flush_start, @writes_ms},
      flush: flush
    }
  end

  @spec deadline(limit()) :: integer()
  def deadline({cutoff, max_ms}), do: min(cutoff, System.monotonic_time(:millisecond) + max_ms)

  @spec remaining(integer()) :: non_neg_integer()
  def remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  defp flush_plan(_now, overall, nil, _spool_buffer), do: {overall, nil}

  defp flush_plan(now, overall, drain_ms, spool_buffer) do
    sync_ms = if spool_buffer, do: @sync_ms, else: 0
    reserve = min(sync_ms + @cleanup_ms, div(max(overall - now, 0), 2))
    sync_reserve = min(sync_ms, div(reserve, 2))
    queues_end = overall - reserve
    sync_end = queues_end + sync_reserve

    {queues_end,
     %{queues: {queues_end, drain_ms}, sync: {sync_end, sync_ms}, cleanup: {overall, @cleanup_ms}}}
  end
end
