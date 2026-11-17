defmodule Logflare.AlertIssues.NotificationWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.AlertIssues.NotificationWorker
  alias Logflare.AlertIssues.Notifier
  alias Logflare.Backends
  alias Tesla.Adapter.Finch

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    previous = Application.get_env(:logflare, Notifier)
    Application.put_env(:logflare, Notifier, url: "http://control-plane.test/hook", secret: "s")
    on_exit(fn -> Application.put_env(:logflare, Notifier, previous) end)

    test_pid = self()

    stub(Backends, :ingest_logs, fn events, source ->
      send(test_pid, {:ingested, source, events})
      {:ok, length(events)}
    end)

    args = %{
      "user_id" => user.id,
      "event" => %{
        "event_id" => "e1",
        "type" => "alert_issue.opened",
        "occurred_at" => "2026-11-17T12:00:00Z",
        "policy" => %{"name" => "ops", "receivers" => ["ops-webhook"]},
        "alert_issue" => %{"alert_issue_id" => "a1", "version" => 1}
      }
    }

    %{user: user, args: args}
  end

  defp respond(status) do
    stub(Finch, :call, fn %Tesla.Env{} = env, _opts ->
      {:ok, %Tesla.Env{env | status: status, body: ""}}
    end)
  end

  test "logs a sent delivery to system.alert_issue_notifications", %{args: args, user: user} do
    respond(200)

    assert :ok = perform_job(NotificationWorker, args)

    assert_receive {:ingested, source, [row]}
    assert source.user_id == user.id
    assert source.system_source_type == :alert_issue_notifications

    assert %{
             "event_id" => "e1",
             "alert_issue_id" => "a1",
             "policy_name" => "ops",
             "type" => "alert_issue.opened",
             "receivers" => ["ops-webhook"],
             "status" => "sent",
             "attempts" => 1,
             "http_status" => 200,
             "error" => nil
           } = row

    assert is_integer(row["version"])
  end

  test "retries a failed delivery without logging it", %{args: args} do
    respond(503)

    assert {:error, {:http_status, 503}} = perform_job(NotificationWorker, args, attempt: 1)
    refute_receive {:ingested, _source, _rows}
  end

  test "logs the final failed attempt", %{args: args} do
    respond(503)

    assert {:error, _} =
             perform_job(NotificationWorker, args, attempt: 5, max_attempts: 5)

    assert_receive {:ingested, _source, [%{"status" => "failed", "attempts" => 5} = row]}
    assert row["http_status"] == 503
    assert row["error"] =~ "503"
  end

  test "does nothing when the notifier is not configured", %{args: args} do
    Application.put_env(:logflare, Notifier, url: nil, secret: nil)
    reject(&Finch.call/2)

    assert {:cancel, :notifier_disabled} = perform_job(NotificationWorker, args)
  end
end
