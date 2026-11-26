defmodule Logflare.RecordUsages.ShutdownWorkerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.RecordUsage
  alias Logflare.RecordUsages.ShutdownWorker
  alias Logflare.RecordUsages.Writer

  setup do
    on_exit(fn ->
      {:ok, _} = Supervisor.restart_child(Logflare.Supervisor, ShutdownWorker)
    end)
  end

  test "supervised shutdown drains queued writes before persisting local usage" do
    token = insert(:access_token)
    timestamp = ~U[2026-01-01 00:00:19Z]
    writer = Process.whereis(Writer)

    on_exit(fn ->
      :sys.resume(writer)
      Cache.reset()
    end)

    :sys.suspend(writer)
    Writer.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp))

    shutdown =
      Task.async(fn -> Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker) end)

    refute Task.yield(shutdown, 50)
    :sys.resume(writer)
    assert Task.await(shutdown) == :ok

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: token.id).last_used_at == timestamp
    assert Cache.stream() |> Enum.to_list() == []
  end

  test "shutdown persists completed usage while the writer is unavailable" do
    token = insert(:access_token)
    timestamp = ~U[2026-01-01 00:00:09Z]
    key = {OauthAccessToken, token.id}
    Writer.record(key, DateTime.to_unix(timestamp))
    assert Cache.stream() |> Enum.to_list() == [{key, DateTime.to_unix(timestamp)}]

    on_exit(fn ->
      {:ok, _} =
        Supervisor.restart_child(Logflare.ContextCache.Supervisor, Writer)
    end)

    assert :ok =
             Supervisor.terminate_child(
               Logflare.ContextCache.Supervisor,
               Writer
             )

    assert :ok = Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: token.id).last_used_at == timestamp
    assert Cache.stream() |> Enum.to_list() == []
  end

  test "a stalled flush cannot extend the shutdown timeout" do
    parent = self()

    stub(RecordUsages, :flush, fn ->
      send(parent, {:flushing, self()})
      Process.sleep(:infinity)
    end)

    started_at = System.monotonic_time(:millisecond)

    assert capture_log(fn ->
             shutdown =
               Task.async(fn ->
                 Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)
               end)

             assert_receive {:flushing, flush_task}, 1_000
             ref = Process.monitor(flush_task)
             assert Task.await(shutdown, 12_000) == :ok
             assert_receive {:DOWN, ^ref, :process, ^flush_task, _reason}
           end) =~ "Record usage shutdown flush timed out after 10000ms"

    assert System.monotonic_time(:millisecond) - started_at < 12_000
  end

  test "a database failure during flush logs the failure without blocking shutdown" do
    token = insert(:access_token)
    RecordUsages.record(OauthAccessToken, token.id)
    snapshot = Cache.stream() |> Enum.to_list()

    expect(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    log =
      capture_log(fn ->
        assert :ok = Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)
      end)

    assert log =~ "Record usage shutdown flush failed"
    assert log =~ "database unavailable"

    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
    assert Cache.stream() |> Enum.to_list() == snapshot
  end
end
