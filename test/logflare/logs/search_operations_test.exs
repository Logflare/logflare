defmodule Logflare.Logs.SearchOperationsTest do
  use Logflare.DataCase, async: false

  alias Logflare.Logs.SearchOperations
  alias Logflare.Sources.Source.BigQuery.Schema

  describe "unnesting metadata if present" do
    setup do
      insert(:plan)
      source = insert(:source, user: insert(:user), bq_table_id: "1")

      [
        so: %Logflare.Logs.SearchOperation{
          source: source,
          querystring: "SELECT * FROM test",
          chart_data_shape_id: nil,
          tailing?: false,
          partition_by: :timestamp
        }
      ]
    end

    test "unnest_log_level/1", %{so: so} do
      so =
        so
        |> SearchOperations.apply_query_defaults()
        |> SearchOperations.unnest_log_level()

      {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

      refute sql =~ "UNNEST(t0.metadata)"
    end

    test "unnest_log_level/1 with metadata", %{so: so} do
      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _project_id, _dataset_id, _table_name, _opts ->
        {:ok, %{}}
      end)

      Logflare.Mailer
      |> expect(:deliver, 1, fn _ -> :ok end)

      pid =
        start_supervised!(
          {Schema,
           source: so.source,
           bigquery_project_id: "some-project",
           bigquery_dataset_id: "some-dataset"}
        )

      Schema.update(pid, build(:log_event, metadata: %{"level" => "value"}))

      TestUtils.retry_assert(fn ->
        Cachex.clear(Logflare.SourceSchemas.Cache)

        so =
          so
          |> SearchOperations.apply_query_defaults()
          |> SearchOperations.unnest_log_level()

        {sql, _} = Logflare.EctoQueryBQ.SQL.to_sql_params(so.query)

        assert sql =~ "UNNEST(t0.metadata)"
      end)
    end
  end
end
