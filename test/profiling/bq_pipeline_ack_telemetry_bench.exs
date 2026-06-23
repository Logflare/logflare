# Measures the per-batch telemetry-label work that PR #3619 moves out of ack/3 and
# into handle_batch/4.
#
# old (ack/3): for every successful event, :ets.lookup(tid, id) copies the full
#   LogEvent out of ETS onto the ack process heap solely to resolve telemetry
#   labels, and get_labels_from_event/2 re-parses source.labels per event.
#     per batch: N x (ets.lookup full-event copy + label-mapping re-parse + extract)
#
# new (handle_batch/4): the events are already in hand via `triples` (the lookup
#   handle_batch already did to build the insert batch), so emit_ingest_telemetry/3
#   adds no lookup — it resolves the label mapping once and reuses it across events.
#     per batch: 1 x get_labels_mapping + N x extract_labels
#
# So the bench isolates the *additional* work each path imposes for label resolution;
# the trailing :telemetry.execute/3 is identical in both and is excluded as noise.
#
# Body-size axis: the eliminated lookup is a deep copy whose cost scales with the
# event's heap footprint. "small"/"medium" bodies are maps of short (<=64 byte, so
# heap-binary) strings — fully copied, no sharing. "large blob" carries one big refc
# binary (>64 bytes): shared by refcount on read, so its copy is cheap despite a large
# external_size — this is why memory/time for the old path do not track body bytes 1:1.
#
# Recorded on branch perf/bq-pipeline-telemetry-handle-batch @ 750bcd526 (per batch of 500):
#
#   source WITHOUT labels        old (ack)        new (handle_batch)   speedup   mem (old -> new)
#     small (~3 fields)          115.33 us          8.76 us            13.2x     891 KB ->  51 KB
#     medium (~25 fields)        239.78 us          9.95 us            24.1x    1.96 MB ->  50 KB
#     large blob (8KB refc)      272.53 us         19.04 us            14.3x     963 KB ->  51 KB
#
#   source WITH labels           old (ack)        new (handle_batch)   speedup   mem (old -> new)
#     small (~3 fields)          243.83 us         30.60 us             8.0x    1.06 MB -> 107 KB
#     medium (~25 fields)        438.58 us         65.45 us             6.7x    2.15 MB -> 107 KB
#     large blob (8KB refc)      339.15 us         75.79 us             4.5x    1.13 MB -> 107 KB
#
# Note the refc-sharing effect: the old path's memory for the 8KB-blob body (963 KB / 1.13 MB)
# is *below* the 25-small-field body (1.96 / 2.15 MB) despite a far larger external_size —
# the blob is shared by refcount on each lookup, the many small fields are copied in full.

alias Logflare.Backends.IngestEventQueue
alias Logflare.Factory
alias Logflare.LogEvent, as: LE
alias Logflare.Sources

batch_size = 500

user = Factory.insert(:user)

# Two source label configs. The labelled source resolves "lvl" from body["metadata"]["level"].
source_no_labels = Factory.insert(:source, user: user)
source_with_labels = Factory.insert(:source, user: user, labels: "lvl=m.level")

# Build a metadata map of `field_count` short string fields plus, optionally, one large
# refc binary. "level" is always present so the labelled source resolves a real value.
make_metadata = fn field_count, blob_bytes ->
  base =
    for i <- 1..field_count, into: %{} do
      {"field_#{i}", "value_#{i}"}
    end

  base = Map.put(base, "level", "error")
  if blob_bytes > 0, do: Map.put(base, "blob", String.duplicate("x", blob_bytes)), else: base
end

body_variants = [
  {"small (~3 fields)", make_metadata.(3, 0)},
  {"medium (~25 small fields)", make_metadata.(25, 0)},
  {"large blob (8KB refc binary)", make_metadata.(3, 8_192)}
]

# For each (source-config, body-variant), stand up a live queue keyed by an owner pid
# (mirrors BufferProducer's {sid, bid, self()}), insert a full batch through the real
# add_to_table path so rows carry the current tuple shape, and capture the ids (old path
# input) and triples (new path input — the events already in hand in handle_batch).
build_input = fn source, metadata ->
  owner = spawn(fn -> :timer.sleep(:infinity) end)
  key = {source.id, nil, owner}
  IngestEventQueue.upsert_tid(key)
  tid = IngestEventQueue.get_tid(key)

  events =
    for _ <- 1..batch_size do
      %LE{} = Factory.build(:log_event, source: source, metadata: metadata)
    end

  IngestEventQueue.add_to_table({key, tid}, events)

  ids = Enum.map(events, & &1.id)
  triples = Enum.map(events, fn le -> {nil, le, :erlang.external_size(le.body)} end)

  %{tid: tid, ids: ids, triples: triples, source: source}
end

# The per-event ack work the PR removes: copy the event out of ETS, then resolve labels.
old_ack_path = fn %{tid: tid, ids: ids, source: source} ->
  Enum.each(ids, fn id ->
    case :ets.lookup(tid, id) do
      [{_id, _status, le, _size, _claim, _claimed_at}] ->
        Sources.get_labels_from_event(source, le)

      [] ->
        :ok
    end
  end)
end

# The new handle_batch work: mapping resolved once, events reused from `triples`.
new_handle_batch_path = fn %{triples: triples, source: source} ->
  mapping = Sources.get_labels_mapping(source)

  Enum.each(triples, fn {_msg, le, _size} ->
    Sources.extract_labels(mapping, le)
  end)
end

run = fn label, source, build_fn ->
  inputs =
    Map.new(body_variants, fn {name, metadata} ->
      {name, build_fn.(source, metadata)}
    end)

  # credo:disable-for-next-line
  IO.puts("\n=== #{label} (batch=#{batch_size}) ===\n")

  Benchee.run(
    %{
      "old: per-event ack ETS lookup + get_labels_from_event" => old_ack_path,
      "new: handle_batch reuse triples + extract_labels" => new_handle_batch_path
    },
    inputs: inputs,
    time: 5,
    warmup: 2,
    memory_time: 3,
    reduction_time: 3
  )
end

run.("source WITHOUT labels", source_no_labels, build_input)
run.("source WITH labels", source_with_labels, build_input)
