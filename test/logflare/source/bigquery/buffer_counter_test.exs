defmodule Logflare.Source.BigQuery.BufferCounterTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Backends

  setup do
    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)
    insert(:plan)
    user = insert(:user)

    source = insert(:source, user: user)
    name = Backends.via_source(source, {BufferCounter, nil})

    start_supervised!(
      {BufferCounter,
       [
         source_id: source.id,
         backend_id: nil,
         name: name
       ]}
    )

    [name: name, source: source]
  end

  test "inc log event count", %{name: name} do
    assert {:ok, 1} = BufferCounter.inc(name, 1)
    assert {:ok, 5} = BufferCounter.inc(name, 4)
    assert 5 = BufferCounter.len(name)
  end

  test "decr log event count", %{name: name} do
    assert {:ok, 4} = BufferCounter.inc(name, 4)
    assert {:ok, 3} = BufferCounter.decr(name, 1)
    assert 3 = BufferCounter.len(name)
  end

  test "errors when buffer is full", %{name: name} do
    assert {:ok, 2} = BufferCounter.inc(name, 2)
    assert %{len: 2, discarded: 0} = BufferCounter.get_counts(name)
    assert {:ok, 4998} = BufferCounter.inc(name, 4996)
    assert {:ok, 9996} = BufferCounter.inc(name, 4998)
    assert {:error, :buffer_full} = BufferCounter.inc(name, 4998)
    assert %{len: 9996, discarded: 4998} = BufferCounter.get_counts(name)
  end
end
