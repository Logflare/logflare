defmodule Logflare.Endpoints.ClickHouseSqlSettingsTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints.ClickHouseSqlSettings
  alias Logflare.Sql.DialectTransformer.ClickHouse, as: ClickHouseTransformer
  alias Logflare.Sql.Parser

  @limits %{
    "max_execution_time" => 10,
    "max_memory_usage" => 1000,
    "max_rows_to_read" => 100,
    "max_bytes_to_read" => 10_000,
    "max_threads" => 4,
    "priority" => 5
  }

  test "outer SQL requests can tighten every resource limit but cannot relax it" do
    for {key, baseline} <- Map.delete(@limits, "priority"),
        requested <- [1, baseline, baseline * 2],
        validated <- [false, true] do
      assert {:ok, sql} =
               ClickHouseSqlSettings.enforce(
                 "SELECT a FROM t SETTINGS #{key} = #{requested}",
                 @limits,
                 validated_clickhouse_sql: validated
               )

      assert sql =~ "#{key} = #{min(baseline, requested)}"
      assert length(Regex.scan(~r/\b#{key}\s*=/i, sql)) == 1
    end
  end

  test "priority can reduce urgency but cannot increase it" do
    for requested <- [1, 5, 10] do
      assert {:ok, sql} =
               ClickHouseSqlSettings.enforce(
                 "SELECT a FROM t SETTINGS priority = #{requested}",
                 @limits
               )

      assert sql =~ "priority = #{max(5, requested)}"
    end
  end

  test "requires explicit inherited baselines even with no other policy" do
    for key <- Map.keys(@limits), policy <- [%{}, Map.delete(@limits, key)] do
      expected = "ClickHouse setting #{key} requires an explicit inherited policy limit"

      assert {:error, ^expected} =
               ClickHouseSqlSettings.enforce("SELECT 1 AS a SETTINGS #{key} = 1", policy)
    end
  end

  test "supports fractional timeouts, exact integer bounds and derived overflow modes" do
    for value <- ["0.25", "1e-6", "9223372036854775807"] do
      assert {:ok, sql} =
               ClickHouseSqlSettings.enforce(
                 "SELECT 1 AS a SETTINGS max_execution_time = #{value}",
                 %{
                   "max_execution_time" => 9_223_372_036_854_775_807
                 }
               )

      assert sql =~ "timeout_overflow_mode = 'throw'"
      assert {:ok, [_statement]} = Parser.parse("clickhouse", sql)
    end

    assert {:ok, sql} =
             ClickHouseSqlSettings.enforce("SELECT 1 AS a SETTINGS max_rows_to_read = 1", %{
               "max_rows_to_read" => 10
             })

    assert sql =~ "read_overflow_mode = 'throw'"
  end

  test "rejects unlimited values, wrong types, expressions and numeric overflow" do
    for key <- Map.keys(@limits),
        value <- ["0", "-1", "'1'", "true", "1 + 1", "{limit:Int64}", "1e1000"] do
      assert {:error, _reason} =
               ClickHouseSqlSettings.enforce("SELECT 1 AS a SETTINGS #{key} = #{value}", @limits)
    end

    for key <- Map.keys(Map.delete(@limits, "max_execution_time")) do
      assert {:error, _} =
               ClickHouseSqlSettings.enforce("SELECT 1 AS a SETTINGS #{key} = 1.5", @limits)
    end

    assert {:error, _} =
             ClickHouseSqlSettings.enforce(
               "SELECT 1 AS a SETTINGS max_threads = 9223372036854775808",
               @limits
             )
  end

  test "normalizes setting names and rejects case-insensitive duplicate requests" do
    assert {:ok, sql} =
             ClickHouseSqlSettings.enforce("SELECT 1 AS a SETTINGS MAX_THREADS = 1", @limits)

    assert sql =~ "max_threads = 1"
    refute sql =~ "MAX_THREADS"

    for key <- Map.keys(@limits) do
      expected = "Duplicate ClickHouse setting #{key}"

      assert {:error, ^expected} =
               ClickHouseSqlSettings.enforce(
                 "SELECT 1 AS a SETTINGS #{key} = 1, #{String.upcase(key)} = 2",
                 @limits
               )
    end
  end

  test "rejects requests nested in CTEs, derived tables and EXPLAIN" do
    for query <- [
          "WITH src AS (SELECT a FROM t SETTINGS max_threads = 1) SELECT a FROM src",
          "SELECT a FROM (SELECT a FROM t SETTINGS max_threads = 1)",
          "EXPLAIN WITH src AS (SELECT a FROM t SETTINGS max_threads = 1) SELECT a FROM src"
        ],
        policy <- [%{}, @limits] do
      assert {:error, reason} = ClickHouseSqlSettings.enforce(query, policy)
      assert reason =~ "only allowed in the outer SETTINGS clause"
    end
  end

  test "preserves EXPLAIN, native parameters and existing optimizer settings" do
    for prefix <- ["", "EXPLAIN ", "EXPLAIN ANALYZE "] do
      query =
        prefix <>
          "SELECT {$0:Int64} AS a SETTINGS optimize_read_in_order = 1, max_threads = 1"

      assert {:ok, sql} = ClickHouseSqlSettings.enforce(query, @limits)
      assert String.starts_with?(sql, prefix <> "SELECT")
      assert sql =~ "{$0: Int64}"
      assert sql =~ "optimize_read_in_order = 1"
      assert sql =~ "max_threads = 1"
      assert length(Regex.scan(~r/\bSETTINGS\b/, sql)) == 1
    end
  end

  test "does not mistake setting names in strings or comments for requests" do
    query = "SELECT 'SETTINGS max_threads = 0' AS a /* priority = 0 */"
    assert {:ok, sql} = ClickHouseSqlSettings.enforce(query, @limits)
    assert sql =~ "'SETTINGS max_threads = 0' AS a"
    assert sql =~ "max_threads = 4"
  end

  test "does not permit consumer changes to enforced non-request keys" do
    assert {:error, reason} =
             ClickHouseSqlSettings.enforce(
               "SELECT 1 AS a SETTINGS max_rows_to_read = 1, read_overflow_mode = 'break'",
               @limits
             )

    assert reason =~ "read_overflow_mode is enforced"
  end

  test "rejects multiple statements and non-query requests" do
    for query <- [
          "SELECT 1 AS a SETTINGS max_threads = 1; SELECT 2",
          "EXPLAIN TABLE t SETTINGS max_threads = 1"
        ] do
      assert {:error, _} = ClickHouseSqlSettings.enforce(query, @limits)
    end
  end

  test "endpoint requests resolve against the current requested class and operator policy" do
    backend = %Backend{
      config: %{
        query_class_settings: %{
          "default" => %{"priority" => 5, "max_threads" => 8},
          "api_paid" => %{"priority" => 10, "max_threads" => 4},
          "dashboard_logs_paid" => %{"priority" => 1}
        },
        read_only_urls: %{"api_paid" => "http://paid.example:8123"},
        default_read_cluster: "api_paid"
      }
    }

    query = "SELECT 1 AS a SETTINGS priority = 2, max_threads = 6"

    for {label, priority, threads} <- [
          {"api_paid", 10, 4},
          {"dashboard_logs_paid", 2, 6},
          {nil, 5, 6},
          {"unknown", 5, 6}
        ] do
      assert {:ok, sql} =
               ClickHouseAdaptor.prepare_query(backend, query,
                 query_type: :endpoint,
                 read_cluster: label,
                 validated_clickhouse_sql: true
               )

      assert sql =~ "priority = #{priority}"
      assert sql =~ "max_threads = #{threads}"
    end

    assert {:ok, sql} =
             ClickHouseAdaptor.prepare_query(backend, query,
               query_type: :endpoint,
               read_cluster: "api_paid",
               enforced_clickhouse_settings: %{"max_threads" => 1, "priority" => 15}
             )

    assert sql =~ "max_threads = 1"
    assert sql =~ "priority = 15"

    updated = put_in(backend.config.query_class_settings["api_paid"]["max_threads"], 2)

    assert {:ok, sql} =
             ClickHouseAdaptor.prepare_query(updated, query,
               query_type: :endpoint,
               read_cluster: "api_paid"
             )

    assert sql =~ "max_threads = 2"
  end

  test "internal endpoint result-limit wrappers retain outer request scope" do
    for query <- [
          "SELECT a FROM t SETTINGS max_threads = 1",
          "SELECT a FROM t LIMIT 2 BY a SETTINGS max_threads = 1",
          "SELECT a FROM t UNION ALL SELECT a FROM s SETTINGS max_threads = 1"
        ] do
      assert {:ok, limited} = ClickHouseTransformer.apply_limit(query, 10)

      assert {:ok, sql} =
               ClickHouseSqlSettings.enforce(limited, @limits, validated_clickhouse_sql: true)

      assert sql =~ "max_threads = 1"
      assert length(Regex.scan(~r/\bSETTINGS\b/, sql)) == 1
    end
  end

  test "debug diagnostics show requested and effective settings" do
    log =
      ExUnit.CaptureLog.capture_log(
        [
          level: :debug,
          format: "$message $metadata",
          metadata: [:clickhouse_requested_settings, :clickhouse_effective_settings]
        ],
        fn ->
          assert {:ok, _} =
                   ClickHouseSqlSettings.enforce(
                     "SELECT 1 AS a SETTINGS max_threads = 8",
                     @limits
                   )
        end
      )

    assert log =~ "Applied lower-only ClickHouse SQL settings"
    assert log =~ "max_threads"
    assert log =~ "8"
    assert log =~ "4"
  end

  test "raw adaptor policy still rejects overrides rather than treating them as endpoint requests" do
    backend = %Backend{config: %{query_class_settings: %{"default" => @limits}}}

    assert {:error, reason} =
             ClickHouseAdaptor.prepare_query(backend, "SELECT 1 AS a SETTINGS priority = 10")

    assert reason =~ "priority is enforced"

    assert {:error, _} =
             ClickHouseAdaptor.prepare_query(
               %Backend{config: %{}},
               "SELECT 1 AS a SETTINGS priority = 1",
               query_type: :endpoint
             )
  end

  test "SQL request validation and sandbox rejection do not require policy or database access" do
    assert {:ok, ast} = Parser.parse("clickhouse", "SELECT 1 AS a SETTINGS max_threads = 1")
    assert {:ok, [_stripped], %{"max_threads" => 1}} = ClickHouseSqlSettings.split_requests(ast)
    assert :ok = ClickHouseSqlSettings.validate_sandboxed(ast, nil)
    assert {:ok, consumer} = Parser.parse("clickhouse", "SELECT 1")

    assert {:error, reason} = ClickHouseSqlSettings.validate_sandboxed(ast, consumer)
    assert reason =~ "not supported in sandboxed SQL"
    assert {:error, _} = ClickHouseSqlSettings.validate_sandboxed(consumer, ast)
    assert :ok = ClickHouseSqlSettings.validate_sandboxed(consumer, consumer)
  end
end
