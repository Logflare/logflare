defmodule Logflare.RecordUsages.ShutdownWorkerTest do
  use Logflare.DataCase, async: false

  import Cachex.Spec, only: [name: 2]
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

  test "supervised shutdown drains in-flight and pending writes before persisting local usage" do
    token = insert(:access_token)
    timestamp = ~U[2026-01-01 00:00:19Z]
    queue = Process.whereis(name(Cache, :locksmith))

    on_exit(fn ->
      :sys.resume(queue)
      Cache.reset()
    end)

    :sys.suspend(queue)
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp) - 19)
    queued_writer(queue)
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp))

    shutdown =
      Task.async(fn -> Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker) end)

    await_draining()

    :sys.resume(queue)
    assert Task.await(shutdown) == :ok

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: token.id).last_used_at == timestamp
    assert Cache.snapshot() == []
  end

  test "shutdown persists completed usage while the writer is unavailable" do
    token = insert(:access_token)
    timestamp = ~U[2026-01-01 00:00:09Z]
    key = {OauthAccessToken, token.id}
    Cache.record(key, DateTime.to_unix(timestamp))
    assert Cache.snapshot() == [{key, DateTime.to_unix(timestamp)}]

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
    assert Cache.snapshot() == []
  end

  test "a stalled batch cannot extend the shutdown timeout and stops with its writer" do
    token = insert(:access_token)
    queue = Process.whereis(name(Cache, :locksmith))

    on_exit(fn ->
      :sys.resume(queue)
      Supervisor.restart_child(Logflare.ContextCache.Supervisor, Writer)
      Cache.reset()
    end)

    :sys.suspend(queue)
    RecordUsages.record(OauthAccessToken, token.id)
    writer = queued_writer(queue)
    started_at = System.monotonic_time(:millisecond)

    assert capture_log(fn ->
             shutdown =
               Task.async(fn ->
                 Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)
               end)

             flush_task = await_draining()
             ref = Process.monitor(flush_task)
             assert Task.await(shutdown, 12_000) == :ok
             assert_receive {:DOWN, ^ref, :process, ^flush_task, _reason}
             assert Process.alive?(writer)

             assert :ok =
                      Supervisor.terminate_child(
                        Logflare.ContextCache.Supervisor,
                        Writer
                      )

             refute Process.alive?(writer)
           end) =~ "Record usage shutdown flush timed out after 10000ms"

    assert System.monotonic_time(:millisecond) - started_at < 12_000
    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
  end

  test "a database failure during flush logs the failure without blocking shutdown" do
    token = insert(:access_token)
    RecordUsages.record(OauthAccessToken, token.id)
    snapshot = Cache.snapshot()

    Repo.query!("""
    ALTER TABLE record_usages
    ADD CONSTRAINT reject_usage_flush CHECK (false) NOT VALID
    """)

    log =
      capture_log(fn ->
        assert :ok = Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)
      end)

    Repo.query!("ALTER TABLE record_usages DROP CONSTRAINT reject_usage_flush")

    assert log =~ "Record usage shutdown flush failed"
    assert log =~ "reject_usage_flush"

    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
    assert Cache.snapshot() == snapshot
  end

  @spec await_draining() :: pid()
  defp await_draining do
    TestUtils.retry_assert(fn ->
      worker = Process.whereis(ShutdownWorker)
      assert is_pid(worker)
      {:links, links} = Process.info(worker, :links)
      {:monitored_by, monitors} = Process.info(Process.whereis(Writer), :monitored_by)
      assert [flush_task] = Enum.filter(monitors, &(&1 in links))
      flush_task
    end)
  end

  @spec queued_writer(pid()) :: pid()
  defp queued_writer(queue) do
    TestUtils.retry_assert(fn ->
      assert {:messages, [{:"$gen_call", {writer, _tag}, _request}]} =
               Process.info(queue, :messages)

      writer
    end)
  end
end
