defmodule Logflare.Logs.SearchTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SingleTenant

  describe "search_events/1" do
    test "with nested and top-level field filters" do
      insert(:plan, name: "Free", type: "standard")
      source = insert(:source, user: insert(:user), bq_table_id: "test_table")
      schema = TestUtils.build_bq_schema(%{"metadata" => %{"level" => "error"}})
      insert(:source_schema, source: source, bigquery_schema: schema)

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _, _, _ ->
        {:ok, TestUtils.gen_bq_response([])}
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

  describe "postgres single tenant integration" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    setup do
      start_supervised!(Logflare.SystemMetricsSup)

      source = insert(:source, user: SingleTenant.get_default_user())
      %{bigquery_schema: schema} = insert(:source_schema, source: source)
      [source: source, schema: schema]
    end

    test "event search filters postgres rows by event_message", %{source: source, schema: schema} do
      matching_message = "postgres-search-match"

      assert {:ok, 2} =
               Backends.ingest_logs(
                 [%{"event_message" => matching_message}, %{"event_message" => "unrelated"}],
                 source
               )

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

    test "event context ranks filtered rows and fetches the nearest 51 matches", %{
      source: source,
      schema: schema
    } do
      timestamp = DateTime.utc_now() |> DateTime.truncate(:second)
      timestamp_us = DateTime.to_unix(timestamp, :microsecond)
      target_id = "00000000-0000-4000-8000-000000000153"

      events =
        for position <- 100..210 do
          %{
            "id" => "00000000-0000-4000-8000-000000000#{position}",
            "timestamp" => if(position == 210, do: timestamp_us + 2_000_000, else: timestamp_us),
            "event_message" =>
              if(position == 154, do: "context skipped", else: "context hit #{position}"),
            "metadata" => %{"position" => position}
          }
        end

      assert {:ok, 111} = Backends.ingest_logs(events, source)
      TestUtils.wait_for_postgres_events(source, source.user, "context ", 111)

      range =
        FilterRule.build(
          path: "timestamp",
          operator: :range,
          values: [timestamp, DateTime.add(timestamp, 1)]
        )

      rules = Lql.decode!("hit", schema) |> Rules.update_timestamp_rules([range])
      so = SO.new(%{source: source, lql_rules: rules, tailing?: false})

      assert {:ok, %{rows: [previous, target, following]}} =
               Search.search_event_context(so, target_id, timestamp, -1..1)

      assert %{"rank" => -1, "metadata" => [%{"position" => 152}]} = previous
      assert %{"rank" => 0, "id" => ^target_id} = target
      assert %{"rank" => 1, "metadata" => [%{"position" => 155}]} = following

      for {intent, positions} <- [previous: 102..152, next: 155..205] do
        page = %{intent: intent, cursor: %{timestamp: timestamp_us, id: target_id}}
        assert {:ok, %{rows: rows}} = Search.search_event_context(so, target_id, timestamp, page)
        assert Enum.map(rows, & &1["event_message"]) == Enum.map(positions, &"context hit #{&1}")
      end
    end
  end
end
