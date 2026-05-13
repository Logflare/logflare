defmodule Logflare.Logs.SearchTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SingleTenant
  alias Logflare.SourceSchemas

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

  describe "postgres single tenant integration" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    setup do
      start_supervised!(Logflare.SystemMetricsSup)

      user = SingleTenant.get_default_user()
      source = insert(:source, user: user)

      assert :ok = Backends.ensure_source_sup_started(source)

      matching_message = "postgres-search-match-#{System.unique_integer([:positive])}"
      non_matching_message = "postgres-search-miss-#{System.unique_integer([:positive])}"

      assert {:ok, 2} =
               Backends.ingest_logs(
                 [
                   %{"event_message" => matching_message},
                   %{"event_message" => non_matching_message}
                 ],
                 source
               )

      schema = TestUtils.build_bq_schema(%{"event_message" => matching_message})

      assert {:ok, _source_schema} =
               SourceSchemas.create_or_update_source_schema(source, %{
                 bigquery_schema: schema,
                 schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
               })

      Cachex.clear(Logflare.SourceSchemas.Cache)

      backend = Backends.get_default_backend(user)

      TestUtils.retry_assert(fn ->
        assert :ok = PostgresAdaptor.test_connection(backend)
      end)

      %{
        user: user,
        source: source,
        matching_message: matching_message,
        schema: schema
      }
    end

    test "event search filters postgres rows by event_message", %{
      source: source,
      matching_message: matching_message,
      schema: schema
    } do
      lql_rules =
        matching_message
        |> Lql.decode!(schema)
        |> Rules.put_new_chart_rule(Rules.default_chart_rule())

      search_operation =
        SO.new(%{
          source: source,
          querystring: matching_message,
          lql_rules: lql_rules,
          chart_data_shape_id: nil,
          tailing?: false,
          type: :events,
          partition_by: :timestamp
        })

      TestUtils.retry_assert(fn ->
        assert {:ok, %{events: events_so}} = Search.search(search_operation)
        assert [%{"event_message" => ^matching_message}] = events_so.rows
      end)
    end
  end
end
