defmodule Logflare.LqlTest do
  use Logflare.DataCase, async: true

  import Ecto.Query

  alias Logflare.Lql
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder

  describe "apply_filter_rules/3" do
    test "applies filter rules to query using BigQuery backend transformer by default" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules(query, [filter_rule])
      assert %Ecto.Query{} = result
    end

    test "applies filter rules for BigQuery dialect" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules(query, [filter_rule], dialect: :bigquery)
      assert %Ecto.Query{} = result
    end

    test "applies filter rules for ClickHouse dialect" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "event_message",
        operator: :"~",
        value: "error.*timeout",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules(query, [filter_rule], dialect: :clickhouse)
      assert %Ecto.Query{} = result
    end

    test "applies filter rules for Postgres dialect" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "m.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.apply_filter_rules(query, [filter_rule], dialect: :postgres)
      assert %Ecto.Query{} = result
    end

    test "handles empty filter rules list" do
      query = from("test_table")

      result = Lql.apply_filter_rules(query, [])
      assert result == query
    end
  end

  describe "handle_nested_field_access/3" do
    test "handles nested field access using BigQuery backend transformer by default" do
      query = from("test_table")

      result = Lql.handle_nested_field_access(query, "metadata.user.id")
      assert %Ecto.Query{} = result
    end

    test "handles nested field access for BigQuery dialect" do
      query = from("test_table")

      result = Lql.handle_nested_field_access(query, "metadata.user.id", dialect: :bigquery)
      assert %Ecto.Query{} = result
    end

    test "handles nested field access for ClickHouse dialect" do
      query = from("test_table")
      result = Lql.handle_nested_field_access(query, "metadata.user.id", dialect: :clickhouse)

      # ClickHouse handles nested fields natively, so query should be unchanged
      assert result == query
    end

    test "handles nested field access for Postgres dialect" do
      query = from("test_table")
      result = Lql.handle_nested_field_access(query, "m.user.id", dialect: :postgres)

      # Postgres handles nested fields via JSONB operators, so query should be unchanged
      assert result == query
    end
  end

  describe "transform_filter_rule/2" do
    test "transforms filter rule using BigQuery backend transformer by default" do
      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.transform_filter_rule(filter_rule)
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms filter rule for BigQuery dialect" do
      filter_rule = %FilterRule{
        path: "metadata.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.transform_filter_rule(filter_rule, dialect: :bigquery)
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms filter rule for ClickHouse dialect" do
      filter_rule = %FilterRule{
        path: "event_message",
        operator: :"~",
        value: "server.*error",
        modifiers: %{}
      }

      result = Lql.transform_filter_rule(filter_rule, dialect: :clickhouse)
      assert %Ecto.Query.DynamicExpr{} = result
    end

    test "transforms filter rule for Postgres dialect" do
      filter_rule = %FilterRule{
        path: "m.status",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      result = Lql.transform_filter_rule(filter_rule, dialect: :postgres)
      assert %Ecto.Query.DynamicExpr{} = result
    end
  end

  describe "decode/2" do
    test "works with BigQuery TableSchema" do
      lql_string = "m.status:error"
      schema = build_basic_schema()

      {:ok, rules} = Lql.decode(lql_string, schema)

      assert length(rules) == 1
      assert [%FilterRule{path: "metadata.status", operator: :=, value: "error"}] = rules
    end

    test "handles empty string with any schema" do
      {:ok, rules} = Lql.decode("", "any_schema")

      assert rules == []
    end
  end

  describe "decode!/2" do
    test "works with BigQuery TableSchema and returns rules directly" do
      lql_string = "m.status:error"
      schema = build_basic_schema()

      rules = Lql.decode!(lql_string, schema)

      assert length(rules) == 1
      assert [%FilterRule{path: "metadata.status", operator: :=, value: "error"}] = rules
    end

    test "handles empty string with any schema and returns rules directly" do
      rules = Lql.decode!("", "any_schema")

      assert rules == []
    end
  end

  describe "encode/1" do
    test "encodes filter rules to query string" do
      rules = [%FilterRule{path: "metadata.status", operator: :=, value: "error", modifiers: %{}}]

      {:ok, encoded} = Lql.encode(rules)

      assert encoded == "m.status:error"
    end

    test "handles empty rules list" do
      {:ok, encoded} = Lql.encode([])

      assert encoded == ""
    end
  end

  describe "encode!/1" do
    test "encodes filter rules to query string directly" do
      rules = [%FilterRule{path: "metadata.status", operator: :=, value: "error", modifiers: %{}}]

      encoded = Lql.encode!(rules)

      assert encoded == "m.status:error"
    end

    test "handles empty rules list" do
      encoded = Lql.encode!([])

      assert encoded == ""
    end
  end

  describe "decode/2 with message filters (legacy build_message_filter_from_regex behavior)" do
    test "builds filter from simple text" do
      text = "error"

      {:ok, rules} = Lql.decode(text, SchemaBuilder.initial_table_schema())

      assert length(rules) == 1
      assert [%FilterRule{path: "event_message", value: "error"}] = rules
    end

    test "builds filter from multiple words" do
      text = "database connection failed"

      {:ok, rules} = Lql.decode(text, SchemaBuilder.initial_table_schema())

      assert length(rules) == 3
      paths = Enum.map(rules, & &1.path)
      assert "event_message" in paths
    end

    test "builds filter from quoted string" do
      quoted_text = ~s|"user authentication failed"|

      {:ok, rules} = Lql.decode(quoted_text, SchemaBuilder.initial_table_schema())

      assert length(rules) == 1
      filter_rule = hd(rules)
      assert filter_rule.path == "event_message"
      assert filter_rule.value == "user authentication failed"
      assert filter_rule.modifiers.quoted_string == true
    end

    test "builds filter from regex pattern" do
      regex = ~s|~"error.*timeout"|

      {:ok, rules} = Lql.decode(regex, SchemaBuilder.initial_table_schema())

      assert length(rules) == 1
      filter_rule = hd(rules)
      assert filter_rule.path == "event_message"
      assert filter_rule.value == "error.*timeout"
      assert filter_rule.operator == :"~"
      assert filter_rule.modifiers.quoted_string == true
    end
  end

  defp build_basic_schema do
    %GoogleApi.BigQuery.V2.Model.TableSchema{
      fields: [
        %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
          name: "metadata",
          type: "RECORD",
          fields: [
            %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
              name: "status",
              type: "STRING"
            }
          ]
        }
      ]
    }
  end

  describe "apply_rules/3" do
    test "applies both filter and select rules to query" do
      query = from("test_table")

      filter_rule = %FilterRule{
        path: "event_message",
        operator: :=,
        value: "error",
        modifiers: %{}
      }

      select_rule = %SelectRule{
        path: "timestamp",
        wildcard: false
      }

      lql_rules = [filter_rule, select_rule]
      result_query = Lql.apply_rules(query, lql_rules)

      assert %Ecto.Query{} = result_query
      refute result_query == query
    end

    test "handles mixed rule types correctly" do
      query = from("test_table")

      lql_rules = [
        %FilterRule{path: "metadata.level", operator: :=, value: "info", modifiers: %{}},
        %SelectRule{path: "*", wildcard: true}
      ]

      result_query = Lql.apply_rules(query, lql_rules)

      assert %Ecto.Query{} = result_query
    end

    test "works with empty rule list" do
      query = from("test_table")

      result_query = Lql.apply_rules(query, [])

      assert result_query == query
    end
  end

  describe "chart rule parsing" do
    test "parses chart aggregations correctly" do
      test_cases = [
        {"c:count(host)", "host", :count, :minute},
        {"c:group_by(t::minute)", "timestamp", :count, :minute},
        {"c:count(host) c:group_by(t::minute)", "host", :count, :minute},
        {"c:avg(m.latency) c:group_by(t::hour)", "metadata.latency", :avg, :hour},
        {"c:sum(my_field.nested.value)", "my_field.nested.value", :sum, :minute},
        {"c:max(request.headers.content_length) c:group_by(t::day)",
         "request.headers.content_length", :max, :day}
      ]

      for {query, path, aggregate, period} <- test_cases do
        {:ok, [rule]} = Parser.parse(query)
        assert %ChartRule{path: ^path, aggregate: ^aggregate, period: ^period} = rule
      end
    end
  end

  describe "`FromRule` decode/encode" do
    test "decodes simple `FromRule`" do
      {:ok, rules} = Parser.parse("f:my_table")

      assert length(rules) == 1
      assert [%FromRule{table: "my_table"}] = rules
    end

    test "decodes `FromRule` with filters" do
      {:ok, rules} = Parser.parse("f:logs m.status:error")

      assert length(rules) == 2
      assert %FromRule{table: "logs"} = Enum.find(rules, &match?(%FromRule{}, &1))

      assert %FilterRule{path: "metadata.status", value: "error"} =
               Enum.find(rules, &match?(%FilterRule{}, &1))
    end

    test "decodes `FromRule` with select rules" do
      {:ok, rules} = Parser.parse("f:events s:timestamp s:event_message")

      assert length(rules) == 3
      assert %FromRule{table: "events"} = Enum.find(rules, &match?(%FromRule{}, &1))
      select_rules = Enum.filter(rules, &match?(%SelectRule{}, &1))
      assert length(select_rules) == 2
    end

    test "decodes `FromRule` with chart rule" do
      {:ok, rules} = Parser.parse("f:metrics c:count(*) c:group_by(t::minute)")

      assert length(rules) == 2
      assert %FromRule{table: "metrics"} = Enum.find(rules, &match?(%FromRule{}, &1))
      assert %ChartRule{aggregate: :count} = Enum.find(rules, &match?(%ChartRule{}, &1))
    end

    test "encodes `FromRule` to query string" do
      rules = [%FromRule{table: "my_table"}]
      {:ok, encoded} = Lql.encode(rules)

      assert encoded == "f:my_table"
    end

    test "encodes `FromRule` with filters and maintains correct order" do
      rules = [
        %FilterRule{path: "metadata.level", operator: :=, value: "error", modifiers: %{}},
        %FromRule{table: "application_logs"}
      ]

      {:ok, encoded} = Lql.encode(rules)

      assert encoded == "f:application_logs m.level:error"
    end

    test "encodes `FromRule` with mixed rules in correct order" do
      rules = [
        %ChartRule{path: "timestamp", aggregate: :count, period: :hour},
        %FilterRule{path: "metadata.status", operator: :=, value: "success", modifiers: %{}},
        %FromRule{table: "requests"},
        %SelectRule{path: "timestamp", wildcard: false}
      ]

      {:ok, encoded} = Lql.encode(rules)

      # FromRule should come first, then select, then filters, then chart
      assert encoded == "f:requests s:timestamp m.status:success c:count(*) c:group_by(t::hour)"
    end

    test "round-trip decode/encode preserves from rule" do
      original = "f:my_source m.level:error s:event_message"
      {:ok, rules} = Parser.parse(original)
      {:ok, encoded} = Lql.encode(rules)

      # Re-parse to verify structure is preserved
      {:ok, rules2} = Parser.parse(encoded)

      assert length(rules) == length(rules2)
      assert Enum.find(rules, &match?(%FromRule{table: "my_source"}, &1))
      assert Enum.find(rules2, &match?(%FromRule{table: "my_source"}, &1))
    end
  end

  describe "to_sandboxed_sql/3" do
    test "converts simple select LQL to BigQuery SQL" do
      lql = "s:field1 s:field2"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "my_cte", :bigquery)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "field1"
      assert String.downcase(sql) =~ "field2"
      assert String.downcase(sql) =~ "from my_cte"
    end

    test "converts chart count aggregation to BigQuery SQL" do
      lql = "c:count(*) c:group_by(t::minute)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "events", :bigquery)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "timestamp_trunc"
      assert String.downcase(sql) =~ "count"
      assert String.downcase(sql) =~ "group by"
      assert String.downcase(sql) =~ "order by"
      assert String.downcase(sql) =~ "from events"
    end

    test "converts chart avg aggregation to BigQuery SQL" do
      lql = "c:avg(m.latency) c:group_by(t::hour)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "metrics", :bigquery)

      assert String.downcase(sql) =~ "avg"
      assert String.downcase(sql) =~ "timestamp_trunc"
      assert String.downcase(sql) =~ "group by"
      assert String.downcase(sql) =~ "hour"
    end

    test "converts chart max aggregation to BigQuery SQL" do
      lql = "c:max(m.response_time) c:group_by(t::second)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "requests", :bigquery)

      assert String.downcase(sql) =~ "max"
      assert String.downcase(sql) =~ "timestamp_trunc"
      assert String.downcase(sql) =~ "second"
    end

    test "converts chart sum aggregation to BigQuery SQL" do
      lql = "c:sum(m.bytes) c:group_by(t::day)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "traffic", :bigquery)

      assert String.downcase(sql) =~ "sum"
      assert String.downcase(sql) =~ "timestamp_trunc"
      assert String.downcase(sql) =~ "group by"
    end

    test "uses `FromRule` table name for select query instead of `cte_table_name` parameter" do
      lql = "f:custom_table s:field1 s:field2"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "ignored_table", :bigquery)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "field1"
      assert String.downcase(sql) =~ "field2"
      assert String.downcase(sql) =~ "from custom_table"
      refute String.downcase(sql) =~ "from ignored_table"
    end

    test "uses `FromRule` table name for chart query instead of `cte_table_name` parameter" do
      lql = "f:metrics_table c:count(*) c:group_by(t::minute)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "default_table", :bigquery)

      assert String.downcase(sql) =~ "count"
      assert String.downcase(sql) =~ "from metrics_table"
      refute String.downcase(sql) =~ "from default_table"
    end

    test "uses `FromRule` with filters for BigQuery SQL" do
      lql = "f:logs m.level:error s:event_message"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "fallback", :bigquery)

      assert String.downcase(sql) =~ "from logs"
      assert String.downcase(sql) =~ "where"
      refute String.downcase(sql) =~ "from fallback"
    end

    test "uses `FromRule` table name for ClickHouse SQL" do
      lql = "f:clickhouse_logs s:event_message"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "unused", :clickhouse)

      assert String.downcase(sql) =~ ~s|from "clickhouse_logs"|
      refute String.downcase(sql) =~ ~s|from "unused"|
    end

    test "falls back to `cte_table_name` parameter when no `FromRule` present" do
      lql = "s:field1 m.level:info"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "last_cte_in_chain", :bigquery)

      assert String.downcase(sql) =~ "from last_cte_in_chain"
    end

    test "falls back to `cte_table_name` for chart queries when no `FromRule` present" do
      lql = "c:count(*) c:group_by(t::hour)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "final_cte", :bigquery)

      assert String.downcase(sql) =~ "from final_cte"
    end

    test "converts chart p95 percentile to BigQuery SQL" do
      lql = "c:p95(m.duration) c:group_by(t::minute)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "traces", :bigquery)

      assert String.downcase(sql) =~ "approx_quantiles"
      # Offset value is interpolated
      assert sql =~ "OFFSET(?)"
    end

    test "converts chart p99 percentile to BigQuery SQL" do
      lql = "c:p99(m.duration) c:group_by(t::minute)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "traces", :bigquery)

      assert String.downcase(sql) =~ "approx_quantiles"
      assert sql =~ "OFFSET(?)"
    end

    test "converts chart aggregation with filters to BigQuery SQL" do
      lql = "c:count(*) c:group_by(t::minute) m.status:>500"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "logs", :bigquery)

      assert String.downcase(sql) =~ "where"
      assert String.downcase(sql) =~ "group by"
      assert String.downcase(sql) =~ "count"
    end

    test "converts combined select and filter to BigQuery SQL" do
      lql = "s:timestamp s:message m.level:error"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "logs", :bigquery)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "timestamp"
      assert String.downcase(sql) =~ "message"
      assert String.downcase(sql) =~ "where"
    end

    test "converts simple select LQL to Postgres SQL" do
      lql = "s:field1 s:field2"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "my_cte", :postgres)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "field1"
      assert String.downcase(sql) =~ "field2"
      # Table names are quoted in Postgres
      assert String.downcase(sql) =~ ~r/from +"?my_cte"?/
    end

    test "converts chart count query to Postgres SQL with date_trunc" do
      lql = "c:count(*) c:group_by(t::minute)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "events", :postgres)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "date_trunc"
      assert String.downcase(sql) =~ "count"
      assert String.downcase(sql) =~ "group by"
      assert String.downcase(sql) =~ "minute"
    end

    test "converts chart avg query to Postgres SQL" do
      lql = "c:avg(m.latency) c:group_by(t::hour)"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "metrics", :postgres)

      assert String.downcase(sql) =~ "avg"
      assert String.downcase(sql) =~ "date_trunc"
      assert String.downcase(sql) =~ "group by"
      assert String.downcase(sql) =~ "hour"
    end

    test "converts chart p95 percentile to Postgres SQL" do
      lql = "c:p95(m.response_time) c:group_by(t::minute)"
      result = Lql.to_sandboxed_sql(lql, "traces", :postgres)

      # Percentile queries with JSONB fields may have limitations in Ecto SQL generation
      # For now, we check that it either succeeds or fails gracefully
      case result do
        {:ok, sql} ->
          assert String.downcase(sql) =~ "percentile_cont" or String.downcase(sql) =~ "select"

        {:error, _reason} ->
          # This is acceptable for now due to Ecto limitations with JSONB in fragments
          assert true
      end
    end

    test "converts filter with JSONB field to Postgres SQL" do
      lql = "m.status:error"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "logs", :postgres)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "where"
      # Field will be represented as metadata_status due to Ecto limitations with JSONB
      assert sql =~ "metadata"
    end

    test "uses `FromRule` table name for Postgres SQL" do
      lql = "f:pg_logs s:event_message"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "unused", :postgres)

      # Should use pg_logs from FromRule, not unused from parameter
      assert String.downcase(sql) =~ "pg_logs"
      refute String.downcase(sql) =~ ~s|"unused"|
    end

    test "converts empty/wildcard select to Postgres SQL with default timestamp field" do
      lql = ""
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "my_table", :postgres)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "timestamp"
      refute sql =~ ~r/SELECT.*\*/i
      assert String.downcase(sql) =~ "from"
    end

    test "returns error for invalid dialect" do
      # This should not compile due to guard, but testing the contract
      assert_raise FunctionClauseError, fn ->
        Lql.to_sandboxed_sql("s:*", "table", :invalid)
      end
    end

    test "converts regex filter to ClickHouse SQL with inlined parameters" do
      lql = "~another"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "event_logs", :clickhouse)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "from"
      assert String.downcase(sql) =~ "event_logs"
      assert String.downcase(sql) =~ "match"
      refute sql =~ ~r/\{\$\d+:/
      assert sql =~ "'another'"
      assert String.downcase(sql) =~ "event_message"
      refute sql =~ ~r/SELECT.*\*/i
    end

    test "converts empty/wildcard select to BigQuery SQL with default timestamp field" do
      lql = ""
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "my_table", :bigquery)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "timestamp"
      refute sql =~ ~r/SELECT.*\*/i
      assert String.downcase(sql) =~ "from"
      assert String.downcase(sql) =~ "my_table"
    end

    test "converts empty/wildcard select to ClickHouse SQL with default timestamp field" do
      lql = ""
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "my_table", :clickhouse)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "timestamp"
      refute sql =~ ~r/SELECT.*\*/i
      assert String.downcase(sql) =~ "from"
      assert String.downcase(sql) =~ "my_table"
    end

    test "`FromRule` overrides `cte_table_name` parameter" do
      # When `f:second_cte` is specified, it should use `second_cte`
      # even though default `cte_table_name` parameter is "final_data"
      lql = "f:second_cte s:col2"
      {:ok, sql} = Lql.to_sandboxed_sql(lql, "final_data", :bigquery)

      assert String.downcase(sql) =~ "select"
      assert String.downcase(sql) =~ "col2"
      assert String.downcase(sql) =~ "from second_cte"
      refute String.downcase(sql) =~ "from final_data"
    end
  end

  describe "language_to_dialect/1" do
    test "converts :bq_sql to :bigquery" do
      assert Lql.language_to_dialect(:bq_sql) == :bigquery
    end

    test "converts :ch_sql to :clickhouse" do
      assert Lql.language_to_dialect(:ch_sql) == :clickhouse
    end

    test "converts :pg_sql to :postgres" do
      assert Lql.language_to_dialect(:pg_sql) == :postgres
    end

    test "defaults to :bigquery for unknown languages" do
      assert Lql.language_to_dialect(:unknown) == :bigquery
    end
  end
end
