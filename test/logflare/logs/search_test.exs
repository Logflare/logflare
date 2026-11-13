defmodule Logflare.Logs.SearchTest do
  use Logflare.DataCase, async: false

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
  alias Logflare.Backends.QueryError
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Logs.Search
  alias Logflare.Logs.SearchOperation, as: SO
  alias Logflare.Lql
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SingleTenant
  alias Logflare.SourceSchemas
  alias Logflare.Sql
  alias Logflare.Sql.Parser
  alias LogflareWeb.Utils

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

  describe "aggs/1" do
    test "decodes BigQuery aggregate columns and preserves the executed SQL for export", %{
      source: source
    } do
      timestamp = DateTime.to_unix(~U[2026-11-04 04:18:00Z], :microsecond)

      response_schema = %GoogleApi.BigQuery.V2.Model.TableSchema{
        fields: [
          %TableFieldSchema{name: "timestamp", type: "INTEGER"},
          %TableFieldSchema{name: "aggregate_0", type: "FLOAT"},
          %TableFieldSchema{name: "aggregate_1", type: "FLOAT"},
          %TableFieldSchema{name: "aggregate_2", type: "INTEGER"}
        ]
      }

      parent = self()

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _project, opts ->
        send(parent, {:executed_chart_sql, opts[:body].query, opts[:body].queryParameters})

        rows = [
          %{
            "timestamp" => Integer.to_string(timestamp),
            "aggregate_0" => "31.25",
            "aggregate_1" => nil,
            "aggregate_2" => "3"
          }
        ]

        {:ok, TestUtils.gen_bq_response(rows, response_schema)}
      end)

      lql_rules = [
        %ChartRule{path: "metadata.latency", aggregate: :sum, value_type: :integer},
        %ChartRule{path: "metadata.response_bytes", aggregate: :avg, value_type: :float},
        Rules.default_chart_rule(),
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~N[2026-11-04 04:18:00], ~N[2026-11-04 04:20:00]],
          modifiers: %{}
        }
      ]

      so = SO.new(%{source: source, lql_rules: lql_rules, querystring: "", tailing?: false})
      assert {:ok, %{aggregates: result}} = Search.aggs(so)
      assert_received {:executed_chart_sql, executed_sql, executed_params}
      assert result.sql_string == executed_sql
      assert result.sql_params == executed_params

      assert Enum.map(
               result.rows,
               &Map.take(&1, ["timestamp", "aggregate_0", "aggregate_1", "aggregate_2"])
             ) == [
               %{
                 "timestamp" => timestamp,
                 "aggregate_0" => 31.25,
                 "aggregate_1" => nil,
                 "aggregate_2" => 3
               }
             ]

      assert result.stats.total_bytes_processed == 1024
      assert {:ok, [_statement]} = Parser.parse("bigquery", result.sql_string)
      assert result.sql_string =~ "GENERATE_TIMESTAMP_ARRAY"

      source = source |> change(name: "aggregate_export_source") |> Repo.update!()

      assert {:ok, source_sql} =
               Sql.transform(
                 :bq_sql,
                 "SELECT event_message FROM aggregate_export_source",
                 source.user
               )

      assert {:ok, expected_tables} = Sql.extract_table_names(source_sql)

      export_sql =
        result.sql_string
        |> Utils.sql_params_to_sql(result.sql_params)
        |> Utils.replace_table_with_source_name(source)

      assert {:ok, transformed} = Sql.transform(:bq_sql, export_sql, source.user)
      assert {:ok, ^expected_tables} = Sql.extract_table_names(transformed)
    end

    test "returns an error rather than displaying a partial chart", %{source: source} do
      error = %QueryError{
        kind: :invalid_query,
        backend: BigQueryAdaptor,
        raw_error: "invalid aggregate"
      }

      stub(BigQueryAdaptor, :execute_query, fn _backend, _query, _opts ->
        {:error, error}
      end)

      lql_rules = [
        %ChartRule{path: "metadata.latency", aggregate: :sum, value_type: :integer},
        %ChartRule{path: "metadata.latency", aggregate: :avg, value_type: :integer}
      ]

      so = SO.new(%{source: source, lql_rules: lql_rules, querystring: "", tailing?: false})
      assert {:error, result} = Search.aggs(so)
      assert result.error == error
      assert result.rows == []
      assert result.chart_rules == so.chart_rules
    end

    test "generates fused BigQuery inputs with distinct percentile and filter parameters", %{
      source: source
    } do
      aggregates = [:sum, :p95, :avg, :p50, :p99, :countd, :max, :count]

      latency_rules =
        Enum.map(
          aggregates,
          &%ChartRule{path: "metadata.latency", aggregate: &1, value_type: :integer}
        )

      rules =
        List.insert_at(latency_rules, 1, Rules.default_chart_rule()) ++
          [
            %ChartRule{path: "metadata.response_bytes", aggregate: :avg, value_type: :integer},
            %FilterRule{path: "metadata.latency", operator: :>=, value: 17},
            %FilterRule{
              path: "timestamp",
              operator: :range,
              values: [~N[2026-11-04 04:18:00], ~N[2026-11-04 04:20:00]],
              modifiers: %{}
            }
          ]

      so = SO.new(%{source: source, lql_rules: rules, querystring: "", tailing?: false})
      assert {:ok, %{aggregates: result}} = Search.aggs(so)

      assert length(Regex.scan(~r/FROM test_table\b/, result.sql_string)) == 3

      for offset <- [50, 95, 99] do
        assert result.sql_string =~ "OFFSET(#{offset})"
      end

      assert {:ok, [_statement]} = Parser.parse("bigquery", result.sql_string)
      assert Enum.count(result.sql_params, &(&1.parameterValue.value == 17)) == 3
    end
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

    test "aggregates preserve series order, filters, real nulls, and empty buckets in exported SQL",
         %{
           source: source,
           matching_message: matching_message
         } do
      timestamp = DateTime.utc_now() |> Map.put(:second, 0) |> DateTime.truncate(:second)
      timestamp = DateTime.add(timestamp, -300, :second)
      backend = Backends.get_default_backend(source.user)

      assert {:ok, _} =
               SharedRepo.with_repo(backend, fn ->
                 SharedRepo.query("""
                 ALTER TABLE "#{PostgresAdaptor.table_name(source)}"
                 ADD COLUMN metadata_latency double precision GENERATED ALWAYS AS
                   ((body->'metadata'->>'latency')::double precision) STORED,
                 ADD COLUMN metadata_response_bytes double precision GENERATED ALWAYS AS
                   ((body->'metadata'->>'response_bytes')::double precision) STORED,
                 ADD COLUMN metadata_request_id text GENERATED ALWAYS AS
                   (body->'metadata'->>'request_id') STORED
                 """)
               end)

      events =
        for {offset, message, latency, response_bytes, request_id} <- [
              {5, matching_message, 10, 7, "first"},
              {13, matching_message, 31, 13, "first"},
              {23, "export-#{matching_message}", nil, 40, "second"},
              {33, "excluded event", 900, 900, "excluded"},
              {65, matching_message, nil, 9, "third"},
              {73, "excluded event", 1000, 1000, "excluded"}
            ] do
          %{
            "event_message" => message,
            "metadata" => %{
              "latency" => latency,
              "response_bytes" => response_bytes,
              "request_id" => request_id
            },
            "timestamp" => timestamp |> DateTime.add(offset, :second) |> DateTime.to_iso8601()
          }
        end

      assert {:ok, 6} = Backends.ingest_logs(events, source)

      min = DateTime.add(timestamp, -60, :second)
      max = DateTime.add(timestamp, 120, :second)

      rules = [
        Rules.default_chart_rule(),
        %ChartRule{path: "metadata.request_id", aggregate: :countd, value_type: :string},
        %ChartRule{path: "metadata.latency", aggregate: :sum, value_type: :float},
        %ChartRule{path: "metadata.latency", aggregate: :avg, value_type: :float},
        %ChartRule{path: "metadata.response_bytes", aggregate: :avg, value_type: :float},
        %FilterRule{path: "event_message", operator: :string_contains, value: matching_message},
        %FilterRule{path: "timestamp", operator: :range, values: [min, max], modifiers: %{}}
      ]

      so = SO.new(%{source: source, lql_rules: rules, querystring: "", tailing?: false})

      first_timestamp = DateTime.to_unix(timestamp, :microsecond)

      expected = [
        %{
          "timestamp" => first_timestamp + 120_000_000,
          "aggregate_0" => 0,
          "aggregate_1" => 0,
          "aggregate_2" => 0,
          "aggregate_3" => 0,
          "aggregate_4" => 0
        },
        %{
          "timestamp" => first_timestamp + 60_000_000,
          "aggregate_0" => 1,
          "aggregate_1" => 1,
          "aggregate_2" => nil,
          "aggregate_3" => nil,
          "aggregate_4" => 9
        },
        %{
          "timestamp" => first_timestamp,
          "aggregate_0" => 3,
          "aggregate_1" => 2,
          "aggregate_2" => 41,
          "aggregate_3" => 20.5,
          "aggregate_4" => 20
        },
        %{
          "timestamp" => first_timestamp - 60_000_000,
          "aggregate_0" => 0,
          "aggregate_1" => 0,
          "aggregate_2" => 0,
          "aggregate_3" => 0,
          "aggregate_4" => 0
        }
      ]

      result =
        TestUtils.retry_assert(fn ->
          assert {:ok, %{aggregates: result}} = Search.aggs(so)
          assert Enum.map(result.rows, &Map.take(&1, Map.keys(hd(expected)))) == expected
          result
        end)

      assert result.series == [
               %{key: "aggregate_0", label: "count(*)"},
               %{key: "aggregate_1", label: "countd(m.request_id)"},
               %{key: "aggregate_2", label: "sum(m.latency)"},
               %{key: "aggregate_3", label: "avg(m.latency)"},
               %{key: "aggregate_4", label: "avg(m.response_bytes)"}
             ]

      assert result.chart_rules == so.chart_rules
      assert result.stats.total_rows == 4

      assert {:ok, exported} =
               PostgresAdaptor.execute_query(backend, {result.sql_string, result.sql_params}, [])

      assert exported.rows == expected

      single = %{
        so
        | chart_rules: [
            %ChartRule{path: "metadata.response_bytes", aggregate: :avg, value_type: :float}
          ]
      }

      assert {:ok, %{aggregates: single_result}} = Search.aggs(single)
      assert single_result.series == [%{key: "value", label: "avg(m.response_bytes)"}]

      assert Enum.map(single_result.rows, &{&1["timestamp"], &1["value"]}) == [
               {first_timestamp + 120_000_000, 0},
               {first_timestamp + 60_000_000, 9},
               {first_timestamp, 20},
               {first_timestamp - 60_000_000, 0}
             ]
    end

    test "retains buckets from any aggregate when equal bounds produce no padding", %{
      source: source
    } do
      timestamp = ~U[2026-11-04 04:18:00.123456Z]

      assert {:ok, 1} =
               Backends.ingest_logs(
                 [
                   %{
                     "event_message" => "observed bucket",
                     "timestamp" => DateTime.to_iso8601(timestamp)
                   }
                 ],
                 source
               )

      count_rule = %{Rules.default_chart_rule() | period: :second}

      distinct_rule = %ChartRule{
        path: "event_message",
        aggregate: :countd,
        value_type: :string,
        period: :second
      }

      for chart_rules <- [[count_rule, distinct_rule], [distinct_rule, count_rule]] do
        rules =
          chart_rules ++
            [
              %FilterRule{
                path: "timestamp",
                operator: :range,
                values: [timestamp, timestamp],
                modifiers: %{}
              },
              %FilterRule{path: "event_message", operator: :=, value: "no matching event"}
            ]

        so = SO.new(%{source: source, lql_rules: rules, querystring: "", tailing?: false})

        result =
          TestUtils.retry_assert(fn ->
            assert {:ok, %{aggregates: result}} = Search.aggs(so)
            assert length(result.rows) == 1
            result
          end)

        [row] = result.rows

        assert row["timestamp"] == DateTime.to_unix(~U[2026-11-04 04:18:00Z], :microsecond)

        assert Enum.map(result.series, &{&1.label, row[&1.key]}) ==
                 Enum.map(chart_rules, fn
                   %{aggregate: :count} -> {"count(*)", 0}
                   %{aggregate: :countd} -> {"countd(event_message)", 1}
                 end)

        backend = Backends.get_default_backend(source.user)

        assert {:ok, exported} =
                 PostgresAdaptor.execute_query(
                   backend,
                   {result.sql_string, result.sql_params},
                   []
                 )

        assert exported.rows == [Map.take(row, ["timestamp", "aggregate_0", "aggregate_1"])]
      end

      empty_rules = [
        count_rule,
        distinct_rule,
        %FilterRule{
          path: "timestamp",
          operator: :range,
          values: [~U[2026-11-04 04:19:00Z], ~U[2026-11-04 04:19:00Z]],
          modifiers: %{}
        }
      ]

      so = SO.new(%{source: source, lql_rules: empty_rules, querystring: "", tailing?: false})
      assert {:ok, %{aggregates: %{rows: []}}} = Search.aggs(so)
    end

    test "exported second buckets match chart padding within and across second boundaries", %{
      source: source
    } do
      first_timestamp = DateTime.to_unix(~U[2026-11-04 04:18:00Z], :microsecond)
      count_rule = Rules.default_chart_rule()

      for {min, max, bucket_count} <- [
            {~N[2026-11-04 04:18:00.100], ~N[2026-11-04 04:18:00.900], 2},
            {~N[2026-11-04 04:18:00.900], ~N[2026-11-04 04:18:01.100], 2},
            {~N[2026-11-04 04:18:00.100], ~N[2026-11-04 04:18:02.900], 3}
          ] do
        rules = [
          %{count_rule | period: :second},
          %ChartRule{
            path: "event_message",
            aggregate: :countd,
            value_type: :string,
            period: :second
          },
          %FilterRule{path: "timestamp", operator: :range, values: [min, max], modifiers: %{}}
        ]

        so = SO.new(%{source: source, lql_rules: rules, querystring: "", tailing?: false})
        assert {:ok, %{aggregates: result}} = Search.aggs(so)

        expected =
          for offset <- (bucket_count - 1)..0//-1 do
            %{
              "timestamp" => first_timestamp + offset * 1_000_000,
              "aggregate_0" => 0,
              "aggregate_1" => 0
            }
          end

        assert Enum.map(result.rows, &Map.take(&1, Map.keys(hd(expected)))) == expected
        backend = Backends.get_default_backend(source.user)

        assert {:ok, exported} =
                 PostgresAdaptor.execute_query(
                   backend,
                   {result.sql_string, result.sql_params},
                   []
                 )

        assert exported.rows == expected
      end
    end
  end
end
