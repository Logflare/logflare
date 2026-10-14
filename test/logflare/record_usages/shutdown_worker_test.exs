defmodule Logflare.RecordUsages.ShutdownWorkerTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.OauthAccessTokens.OauthAccessToken
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache
  alias Logflare.RecordUsages.RecordUsage
  alias Logflare.RecordUsages.ShutdownWorker

  setup do
    on_exit(fn ->
      {:ok, _} = Supervisor.restart_child(Logflare.Supervisor, ShutdownWorker)
    end)
  end

  test "supervised shutdown persists local usage" do
    token = insert(:access_token)
    timestamp = ~U[2026-01-01 00:00:00Z]
    Cache.record({OauthAccessToken, token.id}, DateTime.to_unix(timestamp, :second))

    assert :ok = Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)

    assert Repo.get_by!(RecordUsage, oauth_access_token_id: token.id).last_used_at == timestamp
  end

  test "failed persistence logs the failure without blocking shutdown" do
    token = insert(:access_token)
    RecordUsages.record(OauthAccessToken, token.id)
    stub(RecordUsages, :persist, fn _entries -> raise "database unavailable" end)

    assert capture_log(fn ->
             assert :ok = Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)
           end) =~ "Record usage shutdown flush failed"

    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
  end

  test "a stalled flush times out and stops its task" do
    token = insert(:access_token)
    RecordUsages.record(OauthAccessToken, token.id)
    test_pid = self()

    stub(RecordUsages, :persist, fn _entries ->
      send(test_pid, {:flush_started, self()})
      Process.sleep(:infinity)
    end)

    assert capture_log(fn ->
             assert :ok = Supervisor.terminate_child(Logflare.Supervisor, ShutdownWorker)
           end) =~ "Record usage shutdown flush timed out after 10000ms"

    assert_received {:flush_started, task_pid}
    refute Process.alive?(task_pid)
    assert Repo.get_by(RecordUsage, oauth_access_token_id: token.id) == nil
  end
end
