defmodule Logflare.BigQuery.BqRepoTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.BqRepo

  setup do
    insert(:plan, name: "Free", type: "standard")
    user = insert(:user)
    source = insert(:source, user_id: user.id)

    stub(GoogleApi.BigQuery.V2.Api.Jobs)

    {:ok, source: source, user: user}
  end

  describe "query" do
    test "query_with_sql_and_params returns nil rows for a new empty table", %{user: user} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn
        _conn, "project-id", _opts ->
          {:ok, TestUtils.gen_bq_response([])}
      end)

      {:ok, response} =
        BqRepo.query_with_sql_and_params(
          user,
          "project-id",
          "select timestamp from `my_table`",
          []
        )

      assert response.rows == []
      assert response.total_rows == 0
    end

    test "query_with_sql_and_params add in custom labels", %{user: user} do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 1, fn
        _conn, _, opts ->
          send(pid, opts[:body].labels)
          {:ok, TestUtils.gen_bq_response([])}
      end)

      assert {:ok, _response} =
               BqRepo.query_with_sql_and_params(
                 user,
                 "project-id",
                 "select timestamp from `my_table`",
                 [],
                 labels: %{
                   "custom_tag" => "custom_value"
                 }
               )

      assert_received %{
        "managed_by" => "logflare",
        "custom_tag" => "custom_value"
      }
    end

    test "query_with_sql_and_params executes batch jobs when requested", %{user: user} do
      pid = self()

      response =
        TestUtils.gen_bq_response([])
        |> Map.from_struct()
        |> then(&struct(GoogleApi.BigQuery.V2.Model.GetQueryResultsResponse, &1))

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_insert, 1, fn _conn, "project-id", opts ->
        send(pid, {:priority, opts[:body].configuration.query.priority})
        send(pid, {:labels, opts[:body].configuration.labels})

        {:ok,
         %GoogleApi.BigQuery.V2.Model.Job{
           jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
             jobId: "batch_job_id",
             location: "US",
             projectId: "project-id"
           }
         }}
      end)
      |> expect(:bigquery_jobs_get_query_results, 1, fn _conn,
                                                        "project-id",
                                                        "batch_job_id",
                                                        opts ->
        send(pid, {:poll_opts, opts})
        {:ok, response}
      end)

      assert {:ok, response} =
               BqRepo.query_with_sql_and_params(
                 user,
                 "project-id",
                 "select timestamp from `my_table`",
                 [],
                 job_priority: :batch,
                 labels: %{"custom_tag" => "custom_value"}
               )

      assert response.rows == []
      assert_received {:priority, "BATCH"}
      assert_received {:labels, %{"managed_by" => "logflare", "custom_tag" => "custom_value"}}
      assert_received {:poll_opts, [location: "US", timeoutMs: 25_000]}
    end

    test "query_with_sql_and_params cancels batch jobs on poll timeout", %{user: user} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_insert, 1, fn _conn, "project-id", _opts ->
        {:ok,
         %GoogleApi.BigQuery.V2.Model.Job{
           jobReference: %GoogleApi.BigQuery.V2.Model.JobReference{
             jobId: "batch_job_id",
             location: "US",
             projectId: "project-id"
           }
         }}
      end)
      |> expect(:bigquery_jobs_get_query_results, 1, fn _conn,
                                                        "project-id",
                                                        "batch_job_id",
                                                        opts ->
        assert opts == [location: "US", timeoutMs: 0]
        {:ok, %GoogleApi.BigQuery.V2.Model.GetQueryResultsResponse{jobComplete: false}}
      end)
      |> expect(:bigquery_jobs_cancel, 1, fn _conn, "project-id", "batch_job_id", opts ->
        assert opts == [location: "US"]
        {:ok, %GoogleApi.BigQuery.V2.Model.JobCancelResponse{}}
      end)

      assert {:error, :timeout} =
               BqRepo.query_with_sql_and_params(
                 user,
                 "project-id",
                 "select timestamp from `my_table`",
                 [],
                 job_priority: :batch,
                 timeoutMs: 0
               )
    end

    test "query_with_sql_and_params respects use_query_cache option", %{user: user} do
      pid = self()

      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, 2, fn
        _conn, _, opts ->
          send(pid, {:use_query_cache, opts[:body].useQueryCache})
          {:ok, TestUtils.gen_bq_response([])}
      end)

      assert {:ok, _response} =
               BqRepo.query_with_sql_and_params(
                 user,
                 "project-id",
                 "select timestamp from `my_table`",
                 []
               )

      assert_received {:use_query_cache, true}, "query cache enabled by default"

      assert {:ok, _response} =
               BqRepo.query_with_sql_and_params(
                 user,
                 "project-id",
                 "select timestamp from `my_table`",
                 [],
                 use_query_cache: false
               )

      assert_received {:use_query_cache, false}, "query cache is disabled"
    end
  end
end
