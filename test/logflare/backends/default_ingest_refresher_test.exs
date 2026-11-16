defmodule Logflare.Backends.DefaultIngestRefresherTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.Backends
  alias Logflare.Backends.Backend

  setup :set_mimic_global

  setup do
    insert(:plan)
    user = insert(:user)
    stub(Backends, :maybe_start_consolidated_pipeline, fn _backend -> :ok end)
    {:ok, user: user}
  end

  test "refreshes for the same user run one at a time in arrival order", %{user: user} do
    test_pid = self()
    calls = :counters.new(1, [])

    stub(Backends, :list_default_ingest_backends, fn _user_id ->
      :counters.add(calls, 1, 1)
      send(test_pid, {:reading, self(), :counters.get(calls, 1)})

      receive do
        :release -> []
      end
    end)

    first = Task.async(fn -> Backends.refresh_default_ingest_backends_cache(user.id) end)
    assert_receive {:reading, worker, 1}

    second = await_queued_refresh(worker, user.id)
    refute_received {:reading, _worker, 2}

    send(worker, :release)
    assert :ok = Task.await(first)
    assert_receive {:reading, ^worker, 2}

    send(worker, :release)
    assert :ok = Task.await(second)
  end

  test "a refresh after a disable leaves [] even when an earlier slow refresh read the enabled state",
       %{user: user} do
    backend = insert(:backend, user: user, type: :clickhouse, default_ingest?: true)
    test_pid = self()
    calls = :counters.new(1, [])

    stub(Backends, :list_default_ingest_backends, fn user_id ->
      :counters.add(calls, 1, 1)
      call = :counters.get(calls, 1)
      result = call_original(Backends, :list_default_ingest_backends, [user_id])
      send(test_pid, {:read, self(), call, result})

      if call == 1 do
        receive do
          :release -> result
        end
      else
        result
      end
    end)

    slow = Task.async(fn -> Backends.refresh_default_ingest_backends_cache(user.id) end)
    assert_receive {:read, worker, 1, [%Backend{id: id}]}
    assert id == backend.id

    backend |> Ecto.Changeset.change(default_ingest?: false) |> Repo.update!()
    fast = await_queued_refresh(worker, user.id)

    send(worker, :release)
    assert :ok = Task.await(slow)
    assert :ok = Task.await(fast)

    assert_received {:read, ^worker, 2, []}
    assert [] = Backends.Cache.list_default_ingest_backends(user.id)
  end

  test "a failed primary read keeps the worker alive and leaves the cached lookup untouched",
       %{user: user} do
    test_pid = self()
    cached = [build(:backend, user: user, name: "cached")]
    key = {:list_default_ingest_backends, [user.id]}
    assert ^cached = Backends.Cache.fetch(key, fn -> cached end)

    stub(Backends, :list_default_ingest_backends, fn _user_id ->
      send(test_pid, {:worker, self()})
      raise DBConnection.ConnectionError, "connection not available"
    end)

    log =
      capture_log(fn ->
        assert {:error, %DBConnection.ConnectionError{}} =
                 Backends.refresh_default_ingest_backends_cache(user.id)
      end)

    assert_received {:worker, worker}
    assert log =~ "Default ingest lookup refresh failed"

    stub(Backends, :list_default_ingest_backends, fn _user_id ->
      send(test_pid, {:worker, self()})
      raise Postgrex.Error, message: "boom"
    end)

    capture_log(fn ->
      assert {:error, %Postgrex.Error{}} = Backends.refresh_default_ingest_backends_cache(user.id)
    end)

    assert_received {:worker, ^worker}
    assert Process.alive?(worker)
    assert ^cached = Backends.Cache.list_default_ingest_backends(user.id)
  end

  defp await_queued_refresh(worker, user_id) do
    :erlang.trace(worker, true, [:receive])
    task = Task.async(fn -> Backends.refresh_default_ingest_backends_cache(user_id) end)
    assert_receive {:trace, ^worker, :receive, {:"$gen_call", _from, {:refresh, ^user_id}}}
    :erlang.trace(worker, false, [:receive])
    task
  end
end
