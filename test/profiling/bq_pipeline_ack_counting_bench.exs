# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage: MIX_ENV=test mix run test/profiling/bq_pipeline_ack_counting_bench.exs
#
# Isolates the in-flight reference accounting performed by BigQuery acknowledgement.
# Both jobs operate on the same 500 messages: the old implementation concatenates and
# groups them, while the new implementation counts contiguous reference runs. The
# single-reference input represents steady state. The restart-handoff input models two
# producer lifetimes arriving in processor-demand-sized runs; fully alternating refs
# remain an adversarial comparison.
#
# Median of three runs in the same six-scheduler Linux container with BENCH_TIME=2:
#
#   input              median (old -> new)   memory (old -> new)   reductions
#   single producer        8.79 -> 4.00 us      38.47 -> 0.00 KB    6.28 -> 2.02 K
#   restart handoff       10.04 -> 4.04 us      41.68 -> 0.00 KB    6.29 -> 2.03 K
#   contiguous refs        9.79 -> 4.04 us      44.61 -> 0.00 KB    6.31 -> 2.03 K
#   alternating refs      14.00 -> 7.17 us      50.26 -> 0.00 KB    5.45 -> 4.01 K

batch_size = 500
bench_time = System.get_env("BENCH_TIME", "3") |> String.to_integer()

build_input = fn ref_index ->
  refs = for _ <- 1..4, do: :atomics.new(1, signed: true)

  messages =
    for index <- 0..(batch_size - 1) do
      ref = Enum.at(refs, ref_index.(index))
      %{acknowledger: {nil, nil, %{in_flight_ref: ref}}}
    end

  Enum.split(messages, 450)
end

restart_handoff_ref = fn index ->
  cond do
    index < 200 -> 0
    index < 300 -> 1
    index < 400 -> 0
    true -> 1
  end
end

inputs = %{
  "single producer ref" => build_input.(fn _index -> 0 end),
  "producer restart handoff" => build_input.(restart_handoff_ref),
  "contiguous producer refs" => build_input.(fn index -> div(index, 125) end),
  "alternating producer refs" => build_input.(fn index -> rem(index, 4) end)
}

defmodule BigQueryAckCountingBench do
  def old_decrement({successful, failed}) do
    (successful ++ failed)
    |> Enum.group_by(fn %{acknowledger: {_, _, ack_data}} ->
      Map.get(ack_data, :in_flight_ref)
    end)
    |> Enum.each(fn
      {nil, _messages} -> :ok
      {ref, messages} -> :atomics.sub(ref, 1, length(messages))
    end)
  end

  def new_decrement({successful, failed}) do
    decrement_runs(successful)
    decrement_runs(failed)
  end

  defp decrement_runs(messages), do: decrement_runs(messages, nil, 0)
  defp decrement_runs([], nil, 0), do: :ok
  defp decrement_runs([], ref, run_count), do: decrement(ref, run_count)

  defp decrement_runs(
         [%{acknowledger: {_, _, ack_data}} | messages],
         current_ref,
         run_count
       ) do
    ref = Map.get(ack_data, :in_flight_ref)

    if ref == current_ref do
      decrement_runs(messages, current_ref, run_count + 1)
    else
      decrement(current_ref, run_count)
      decrement_runs(messages, ref, 1)
    end
  end

  defp decrement(nil, _run_count), do: :ok
  defp decrement(ref, run_count), do: :atomics.sub(ref, 1, run_count)
end

Benchee.run(
  %{
    "old: concatenate and group messages" => &BigQueryAckCountingBench.old_decrement/1,
    "new: count contiguous reference runs" => &BigQueryAckCountingBench.new_decrement/1
  },
  inputs: inputs,
  time: bench_time,
  warmup: bench_time,
  memory_time: bench_time,
  reduction_time: bench_time
)
