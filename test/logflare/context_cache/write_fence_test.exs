defmodule Logflare.ContextCache.WriteFenceTest do
  use ExUnit.Case, async: false

  alias Logflare.ContextCache
  alias Logflare.ContextCache.WriteFence
  alias Logflare.Rules
  alias Logflare.Sources

  setup do
    Cachex.clear!(Rules.Cache)
    Cachex.clear!(Sources.Cache)
    :ok
  end

  test "an invalidation during a snapshot prevents every alias from being cached" do
    source_id = 101
    id_key = {:get_by, [[id: source_id]]}
    token_key = {:get_by, [[token: "source-token"]]}
    value = {:cached, %{id: source_id}}

    assert {:ok, snapshot} = WriteFence.begin_snapshot(Sources)
    assert {:ok, 0} = ContextCache.bust_keys([{Sources, source_id}])

    assert {:ok, %{skipped: 1, written: 0}} =
             WriteFence.commit(snapshot, Sources.Cache, [
               {source_id, [{id_key, value}, {token_key, value}]}
             ])

    assert Cachex.get!(Sources.Cache, id_key) == nil
    assert Cachex.get!(Sources.Cache, token_key) == nil
  end

  test "an invalidation after a commit removes the committed entries" do
    source_id = 102
    cache_key = {:get_by, [[id: source_id]]}
    value = {:cached, %{id: source_id}}

    assert {:ok, snapshot} = WriteFence.begin_snapshot(Sources)

    assert {:ok, %{skipped: 0, written: 1}} =
             WriteFence.commit(snapshot, Sources.Cache, [{source_id, [{cache_key, value}]}])

    assert Cachex.get!(Sources.Cache, cache_key) == value
    assert {:ok, 1} = ContextCache.bust_keys([{Sources, source_id}])
    assert Cachex.get!(Sources.Cache, cache_key) == nil
  end

  test "only invalidated primary-key groups are skipped" do
    invalidated_id = 103
    fresh_id = 104
    invalidated_key = {:get_by, [[id: invalidated_id]]}
    fresh_key = {:get_by, [[id: fresh_id]]}

    assert {:ok, snapshot} = WriteFence.begin_snapshot(Sources)
    assert :ok = WriteFence.invalidate([{Sources, invalidated_id}])

    assert {:ok, %{skipped: 1, written: 1}} =
             WriteFence.commit(snapshot, Sources.Cache, [
               {invalidated_id, [{invalidated_key, {:cached, %{id: invalidated_id}}}]},
               {fresh_id, [{fresh_key, {:cached, %{id: fresh_id}}}]}
             ])

    assert Cachex.get!(Sources.Cache, invalidated_key) == nil
    assert Cachex.get!(Sources.Cache, fresh_key) == {:cached, %{id: fresh_id}}
  end

  test "custom keyword invalidations fence cache-specific warmer entries" do
    cache_key = {:list_by_source_id, [202]}

    assert {:ok, snapshot} = WriteFence.begin_snapshot(Rules)
    assert :ok = WriteFence.invalidate([{Rules, [id: 201, source_id: 202]}])

    assert {:ok, %{skipped: 1, written: 0}} =
             WriteFence.commit(snapshot, Rules.Cache, [
               {:unknown, [{cache_key, {:cached, [%{id: 201}]}}]}
             ])

    assert Cachex.get!(Rules.Cache, cache_key) == nil
  end

  test "conflicting duplicate cache keys fail closed" do
    cache_key = {:get_by, [[token: "duplicate"]]}

    assert {:ok, snapshot} = WriteFence.begin_snapshot(Sources)

    assert {:error, :conflicting_entries} =
             WriteFence.commit(snapshot, Sources.Cache, [
               {105, [{cache_key, {:cached, %{id: 105}}}]},
               {106, [{cache_key, {:cached, %{id: 106}}}]}
             ])

    assert Cachex.get!(Sources.Cache, cache_key) == nil
  end

  test "snapshots wait until cache invalidation is ready" do
    {server, active_table, child_id} = isolated_fence_names()

    start_supervised!(
      Supervisor.child_spec(
        {WriteFence, name: server, active_table: active_table, ready?: false},
        id: child_id
      )
    )

    test_pid = self()

    writer =
      spawn_link(fn ->
        {:ok, snapshot} = WriteFence.begin_snapshot(Sources, server)
        send(test_pid, {:snapshot, snapshot})
        receive do: (:stop -> :ok)
      end)

    refute_receive {:snapshot, _snapshot}
    assert :ok = WriteFence.mark_ready(server)
    assert_receive {:snapshot, _snapshot}
    send(writer, :stop)
  end

  test "snapshots from an earlier fence incarnation cannot commit" do
    {server, active_table, child_id} = isolated_fence_names()

    child_spec =
      Supervisor.child_spec(
        {WriteFence, name: server, active_table: active_table, ready?: true},
        id: child_id
      )

    start_supervised!(child_spec)
    assert {:ok, snapshot} = WriteFence.begin_snapshot(Sources, server)
    assert :ok = stop_supervised(child_id)
    start_supervised!(child_spec)

    assert {:error, :unknown_snapshot} =
             WriteFence.commit(snapshot, Sources.Cache, [], server)
  end

  defp isolated_fence_names do
    suffix = System.unique_integer([:positive])

    {
      Module.concat(__MODULE__, "Server#{suffix}"),
      Module.concat(__MODULE__, "ActiveContexts#{suffix}"),
      {__MODULE__, suffix}
    }
  end
end
