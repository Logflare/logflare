defmodule Logflare.Alerting.AlertWorkerTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.Alerting.AlertWorker

  setup do
    insert(:plan, name: "Free")
    old_config = Application.get_env(:logflare, Logflare.Alerting)
    Application.put_env(:logflare, Logflare.Alerting, min_cluster_size: 0, enabled: true)
    on_exit(fn -> Application.put_env(:logflare, Logflare.Alerting, old_config) end)
    {:ok, user: insert(:user)}
  end

  test "perform/1 executes alert and sends notifications on results", %{user: user} do
    alert = insert(:alert, user: user)

    expect_batch_query([%{"testing" => "123"}])

    Logflare.Backends.Adaptor.WebhookAdaptor.Client
    |> expect(:send, fn opts ->
      assert Map.has_key?(opts[:body], "result")
      {:ok, %Tesla.Env{}}
    end)

    Logflare.Backends.Adaptor.SlackAdaptor.Client
    |> expect(:send, fn _url, body ->
      assert Map.has_key?(body, :blocks)
      {:ok, %Tesla.Env{}}
    end)

    assert :ok = perform_job(AlertWorker, %{alert_query_id: alert.id})
  end

  test "perform/1 returns :ok when no query results", %{user: user} do
    alert = insert(:alert, user: user)

    expect_batch_query([])

    assert :ok = perform_job(AlertWorker, %{alert_query_id: alert.id})
  end

  test "perform/1 returns :error when alert not found (deleted)" do
    assert {:error, :not_found} = perform_job(AlertWorker, %{alert_query_id: -1})
  end

  defp expect_batch_query(results) do
    response =
      TestUtils.gen_bq_response(results)
      |> Map.from_struct()
      |> then(&struct(GoogleApi.BigQuery.V2.Model.GetQueryResultsResponse, &1))

    GoogleApi.BigQuery.V2.Api.Jobs
    |> expect(:bigquery_jobs_insert, 1, fn _conn, _proj_id, opts ->
      assert opts[:body].configuration.jobTimeoutMs == 120_000
      assert opts[:body].configuration.query.priority == "BATCH"

      {:ok,
       %GoogleApi.BigQuery.V2.Model.Job{
         jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{jobId: "batch_job_id"}
       }}
    end)
    |> expect(:bigquery_jobs_get_query_results, 1, fn _conn, _proj_id, "batch_job_id", _opts ->
      {:ok, response}
    end)
  end
end
