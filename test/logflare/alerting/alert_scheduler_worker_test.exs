defmodule Logflare.Alerting.AlertSchedulerWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Alerting.AlertSchedulerWorker
  alias Logflare.Alerting.AlertWorker

  setup do
    insert(:plan, name: "Free")
    {:ok, user: insert(:user)}
  end

  test "perform/1 with no alert queries returns :ok, no jobs enqueued" do
    assert :ok = perform_job(AlertSchedulerWorker, %{})
    refute_enqueued(worker: AlertWorker)
  end

  test "perform/1 with alert queries enqueues AlertWorker jobs", %{user: user} do
    alert = insert(:alert, user: user, cron: "0 0 1 * *")

    assert :ok = perform_job(AlertSchedulerWorker, %{})

    assert_enqueued(worker: AlertWorker, args: %{alert_query_id: alert.id})
  end

  test "perform/1 skips invalid cron expressions gracefully", %{user: user} do
    alert = insert(:alert, user: user, cron: "0 0 1 * *")

    Logflare.Repo.update_all(
      from(a in Logflare.Alerting.AlertQuery, where: a.id == ^alert.id),
      set: [cron: "invalid_cron"]
    )

    assert :ok = perform_job(AlertSchedulerWorker, %{})
  end
end
