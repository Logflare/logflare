defmodule Logflare.AlertIssues.NotificationRepeatWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.AlertIssues.NotificationPolicies
  alias Logflare.AlertIssues.NotificationRepeatWorker
  alias Logflare.AlertIssues.NotificationWorker
  alias Logflare.AlertIssues.Notifier
  alias Logflare.AlertIssues.Queries

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    previous = Application.get_env(:logflare, Notifier)
    Application.put_env(:logflare, Notifier, url: "http://control-plane.test/hook", secret: "s")
    on_exit(fn -> Application.put_env(:logflare, Notifier, previous) end)

    {:ok, policy} =
      NotificationPolicies.create(user, %{
        "name" => "pager",
        "repeat_interval_seconds" => 600,
        "receivers" => ["ops-webhook"]
      })

    %{user: user, policy: policy, now: DateTime.utc_now()}
  end

  defp issue(id, attention_at, attrs \\ %{}) do
    Map.merge(
      %{
        "alert_issue_id" => id,
        "version" => 1,
        "status" => "open",
        "severity" => "major",
        "category" => "health",
        "rule_key" => "supabase.edge-function-5xx",
        "identifiers" => %{"project" => "default"},
        "attention_at" => DateTime.to_iso8601(attention_at)
      },
      attrs
    )
  end

  defp us(at), do: DateTime.to_unix(at, :microsecond)
  defp ago(now, seconds), do: DateTime.add(now, -seconds, :second)

  defp repeated_ids do
    all_enqueued(worker: NotificationWorker)
    |> Enum.filter(&(&1.args["event"]["type"] == "alert_issue.repeat"))
    |> Enum.map(& &1.args["event"]["alert_issue"]["alert_issue_id"])
    |> Enum.sort()
  end

  test "repeats open alert issues whose last delivery is older than the interval", %{
    user: user,
    now: now
  } do
    stub(Queries, :list_active, fn %{id: id} ->
      assert id == user.id

      {:ok,
       [
         issue("stale", ago(now, 3600)),
         issue("recent", ago(now, 3600)),
         issue("never-sent-old", ago(now, 900)),
         issue("never-sent-new", ago(now, 60)),
         issue("acked", ago(now, 3600), %{"status" => "acknowledged"})
       ]}
    end)

    stub(Queries, :last_notifications, fn _user, ids ->
      assert "stale" in ids

      {:ok,
       %{
         {"stale", "pager"} => us(ago(now, 700)),
         {"recent", "pager"} => us(ago(now, 120)),
         {"acked", "pager"} => us(ago(now, 3000))
       }}
    end)

    assert :ok = perform_job(NotificationRepeatWorker, %{})
    assert repeated_ids() == ["never-sent-old", "stale"]
  end

  test "reads deliveries from storage on every run, so repeats survive a restart", %{now: now} do
    stub(Queries, :list_active, fn _user -> {:ok, [issue("a1", ago(now, 3600))]} end)

    stub(Queries, :last_notifications, fn _user, _ids ->
      {:ok, %{{"a1", "pager"} => us(ago(now, 60))}}
    end)

    assert :ok = perform_job(NotificationRepeatWorker, %{})
    assert repeated_ids() == []

    stub(Queries, :last_notifications, fn _user, _ids ->
      {:ok, %{{"a1", "pager"} => us(ago(now, 601))}}
    end)

    assert :ok = perform_job(NotificationRepeatWorker, %{})
    assert repeated_ids() == ["a1"]

    assert :ok = perform_job(NotificationRepeatWorker, %{})
    assert repeated_ids() == ["a1"]
  end

  test "skips users without repeating policies and a disabled notifier", %{
    user: user,
    policy: policy
  } do
    reject(&Queries.list_active/1)

    Application.put_env(:logflare, Notifier, url: nil, secret: nil)
    assert :ok = perform_job(NotificationRepeatWorker, %{})

    Application.put_env(:logflare, Notifier, url: "http://control-plane.test/hook", secret: "s")
    {:ok, _} = NotificationPolicies.update(user, policy.id, %{"events" => ["opened"]})
    assert :ok = perform_job(NotificationRepeatWorker, %{})
  end
end
