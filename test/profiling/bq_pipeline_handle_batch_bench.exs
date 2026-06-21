# Measures the per-batch collection work in BigQuery.Pipeline.handle_batch/4 for
# PR #3618 changes #1 and #3. Both run on every batch and scale with batch size
# (up to 500 events), so the win is in allocations/reductions rather than wall time.
#
# #3 — collect log_events + batch_count + batch_size:
#   old: Enum.map (extract le) + length + Enum.sum_by  -> 3 traversals, a full le list
#   new: a single Enum.reduce                          -> 1 traversal, list built via cons
#
# #1 — streaming insert path handing events to stream_batch/2:
#   old: built {le, size} pairs from triples, then re-extracted log_events downstream
#        -> 2 extra traversals + ~2N tuple/cons allocations, all discarded
#   new: passes the already-collected log_events straight through (pass-through)
#
# Caveat: this isolates the list/tuple allocation work over synthetic triples. The le
# and size slots are references, so memory_avg_bytes reflects list spine + tuple
# allocation, not LogEvent body size or live-pipeline GC behaviour.

alias Logflare.Factory
alias Logflare.LogEvent, as: LE

# Tail-recursive collect with separate accumulator args: single pass like the reduce,
# but no per-iteration tuple — only the cons list is allocated, plus one result tuple.
defmodule BenchCollect do
  def run(triples), do: collect(triples, [], 0, 0)
  defp collect([], les, count, bytes), do: {les, count, bytes}

  defp collect([{_msg, le, size} | rest], les, count, bytes) do
    collect(rest, [le | les], count + 1, bytes + size)
  end
end

# {message, log_event, size} mirrors fetch_events_from_messages/3 output. Only le and
# size are read by the code under test; the message slot is a placeholder.
source = Factory.build(:source)

make_triples = fn n ->
  for _ <- 1..n do
    le = %LE{} = Factory.build(:log_event, source: source)
    size = :erlang.external_size(le.body)
    {nil, le, size}
  end
end

triples_inputs = %{
  "batch=50" => make_triples.(50),
  "batch=200" => make_triples.(200),
  "batch=500 (max)" => make_triples.(500)
}

# #1 needs both the triples (old path rebuilds from them) and the already-collected
# log_events (new path receives them directly from the #3 reduce).
stream_inputs =
  Map.new(triples_inputs, fn {k, triples} ->
    {k, {triples, Enum.map(triples, fn {_msg, le, _size} -> le end)}}
  end)

IO.puts("\n=== #3: collect log_events + batch_count + batch_size ===\n")

Benchee.run(
  %{
    "#3 old: map + length + sum_by" => fn triples ->
      log_events = Enum.map(triples, fn {_msg, le, _size} -> le end)
      _batch_count = length(log_events)
      _batch_size = Enum.sum_by(triples, fn {_msg, _le, size} -> size end)
      log_events
    end,
    "#3 new: single reduce" => fn triples ->
      Enum.reduce(triples, {[], 0, 0}, fn {_msg, le, size}, {les, count, bytes} ->
        {[le | les], count + 1, bytes + size}
      end)
    end,
    "#3 alt: tail-recursive (separate accs)" => fn triples ->
      BenchCollect.run(triples)
    end
  },
  inputs: triples_inputs,
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)

IO.puts("\n=== #1: events handed to stream_batch/2 on the streaming path ===\n")

Benchee.run(
  %{
    "#1 old: build {le, size} pairs + re-extract" => fn {triples, _log_events} ->
      event_size_pairs = Enum.map(triples, fn {_msg, le, size} -> {le, size} end)
      Enum.map(event_size_pairs, &elem(&1, 0))
    end,
    "#1 new: pass-through" => fn {_triples, log_events} ->
      log_events
    end
  },
  inputs: stream_inputs,
  time: 5,
  warmup: 2,
  memory_time: 3,
  reduction_time: 3
)
