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
      |> expect(:bigquery_jobs_query, 2, fn
        _conn, "project-id", _opts ->
          {:ok, TestUtils.gen_bq_response([])}

        _conn, "project-id1", _opts ->
          {:ok,
           TestUtils.gen_bq_response(%{"event_message" => "some event message", "a" => "value"})}
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
      query = Ecto.Query.from(f in "mytable", select: "a")

      {:ok, response} = BqRepo.query(user, "project-id1", query, [])
      assert [%{"event_message" => "some event message", "a" => "value"}] = response.rows
      assert response.total_rows == 1
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

    test "query/4 handles ecto sql", %{user: user} do
      GoogleApi.BigQuery.V2.Api.Jobs
      |> expect(:bigquery_jobs_query, fn _conn, "project-id", _opts ->
        {:ok, TestUtils.gen_bq_response(%{"event_message" => "something", "a" => "value"})}
      end)

      query = Ecto.Query.from(f in "mytable", select: "a")
      {:ok, response} = BqRepo.query(user, "project-id", query, [])
      assert [%{"event_message" => "something", "a" => "value"}] = response.rows
      assert response.total_rows == 1
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
