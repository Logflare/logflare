defmodule Logflare.Logs.SearchTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule

  setup do
    insert(:plan, name: "Free", type: "standard")
    schema = TestUtils.build_bq_schema(%{"metadata" => %{"level" => "error"}})
    source = insert(:source, user: insert(:user), bq_table_id: "test_table")
    insert(:source_schema, source: source, bigquery_schema: schema)

    stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _project_id, _opts ->
      {:ok,
       %GoogleApi.BigQuery.V2.Model.QueryResponse{
         rows: [],
         jobComplete: true,
         schema: %GoogleApi.BigQuery.V2.Model.TableSchema{fields: []}
       }}
    end)

    [source: source]
  end

  describe "search_events/1" do
    test "with nested and top-level field filters", %{source: source} do
      GoogleApi.BigQuery.V2.Api.Tables
      |> stub(:bigquery_tables_patch, fn _conn, _project_id, _dataset_id, _table_name, _opts ->
        {:ok, %{}}
      end)

      now = DateTime.utc_now()
      one_hour_ago = DateTime.add(now, -3600, :second)

      lql_rules = [
        %ChartRule{
          path: "timestamp",
          aggregate: :count,
          period: :minute,
          value_type: :datetime
        },
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [one_hour_ago, now],
          modifiers: %{}
        },
        %FilterRule{
          path: "event_message",
          operator: :string_contains,
          value: "stream",
          modifiers: %{}
        },
        %FilterRule{
          path: "event_message",
          operator: :string_contains,
          value: "timedout",
          modifiers: %{negate: true}
        },
        %FilterRule{
          path: "metadata.level",
          operator: :=,
          value: "error",
          modifiers: %{}
        },
        %FilterRule{
          path: "attributes.name",
          operator: :"~",
          value: "jose",
          modifiers: %{}
        }
      ]

      so =
        SO.new(%{
          source: source,
          lql_rules: lql_rules,
          user_id: source.user_id,
          querystring: "",
          tailing?: false,
          chart_data_shape_id: nil
        })

      result = Search.search(so)

      assert {:ok, %{events: result_so}} = result
      assert result_so.query != nil
      refute result_so.error

      {:ok, {sql, _}} = BigQueryAdaptor.ecto_to_sql(result_so.query, [])

      # Reference the top level fields correctly, verifying that
      # apply_select_rules/1 is performed after apply_filter_rules/1
      assert sql =~ "UNNEST(t0.metadata)"
      assert sql =~ "STRPOS(t0.event_message"
    end
  end
end
