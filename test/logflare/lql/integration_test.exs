defmodule Logflare.Lql.IntegrationTest do
  use Logflare.DataCase, async: true

  alias Logflare.Logs.SearchOperation
  alias Logflare.Lql
  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule
  alias Logflare.Lql.Parser
  alias Logflare.Source.BigQuery.SchemaBuilder

  @default_schema SchemaBuilder.initial_table_schema()

  describe "end-to-end LQL processing" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source, user: user}
    end

    test "parses and encodes simple filter", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"status" => "success"}})
      lql_string = "m.status:success"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %FilterRule{path: "metadata.status", operator: :=, value: "success"} = rule
    end

    test "parses and encodes complex filter with multiple conditions", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"status" => "success", "latency" => 100}})
      lql_string = "m.status:success m.latency:>100 event_message:error"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      # Note: encoding might reorder filters
      assert String.contains?(encoded_string, "m.status:success")
      assert String.contains?(encoded_string, "m.latency:>100")
      assert String.contains?(encoded_string, "error")

      assert length(parsed_rules) == 3
    end

    test "parses and encodes chart rules", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"latency" => 100.0}})
      lql_string = "c:avg(m.latency) c:group_by(t::minute)"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %ChartRule{
               path: "metadata.latency",
               aggregate: :avg,
               period: :minute,
               value_type: :float
             } = rule
    end

    test "handles timestamp filters with various formats", %{source: _source} do
      test_cases = [
        "t:today",
        "t:>2023-01-01",
        "t:2023-01-01..2023-01-02",
        "t:last@5m"
      ]

      for lql_string <- test_cases do
        {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
        encoded_string = Lql.encode!(parsed_rules)

        assert length(parsed_rules) == 1
        [rule] = parsed_rules
        assert %FilterRule{path: "timestamp"} = rule

        # Should be able to re-parse encoded string
        {:ok, reparsed_rules} = Parser.parse(encoded_string, @default_schema)
        assert length(reparsed_rules) == 1
      end
    end

    test "creates SearchOperation from LQL string", %{source: source} do
      schema = build_schema(%{"metadata" => %{"status" => "error", "latency" => 500}})
      lql_string = "m.status:error m.latency:>500"
      {:ok, lql_rules} = Parser.parse(lql_string, schema)

      search_op =
        SearchOperation.new(%{
          source: source,
          querystring: lql_string,
          lql_rules: lql_rules,
          tailing?: false,
          partition_by: :timestamp,
          type: :events
        })

      assert search_op.source == source
      assert search_op.querystring == lql_string
      assert search_op.lql_rules == lql_rules
      assert length(search_op.lql_meta_and_msg_filters) == 2
      assert search_op.lql_ts_filters == []
      assert search_op.chart_rules == []
    end

    test "SearchOperation separates filter types correctly", %{source: source} do
      schema = build_schema(%{"metadata" => %{"status" => "error"}})
      lql_string = "m.status:error event_message:warning t:today"
      {:ok, lql_rules} = Parser.parse(lql_string, schema)

      search_op =
        SearchOperation.new(%{
          source: source,
          querystring: lql_string,
          lql_rules: lql_rules,
          tailing?: false,
          partition_by: :timestamp,
          type: :events
        })

      assert length(search_op.lql_meta_and_msg_filters) == 2
      assert length(search_op.lql_ts_filters) == 1
      assert search_op.chart_rules == []
    end

    test "SearchOperation handles chart rules", %{source: source} do
      schema = build_schema(%{"metadata" => %{"status" => "error", "latency" => 100.0}})
      lql_string = "m.status:error c:avg(m.latency) c:group_by(t::minute)"
      {:ok, lql_rules} = Parser.parse(lql_string, schema)

      search_op =
        SearchOperation.new(%{
          source: source,
          querystring: lql_string,
          lql_rules: lql_rules,
          tailing?: false,
          partition_by: :timestamp,
          type: :aggregates
        })

      assert length(search_op.lql_meta_and_msg_filters) == 1
      assert length(search_op.chart_rules) == 1
      assert search_op.type == :aggregates
    end

    test "handles regex patterns with escaping", %{source: _source} do
      lql_string = ~S|~"user \"admin\" login"|

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %FilterRule{
               path: "event_message",
               operator: :"~",
               value: ~S(user \"admin\" login),
               modifiers: %{quoted_string: true}
             } = rule
    end

    test "handles negated filters", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"status" => "error"}})
      lql_string = "-m.status:error -event_message:warning"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      # Note: encoding might reorder filters
      assert String.contains?(encoded_string, "-m.status:error")
      assert String.contains?(encoded_string, "-warning")
      assert length(parsed_rules) == 2

      for rule <- parsed_rules do
        assert %FilterRule{modifiers: %{negate: true}} = rule
      end
    end

    test "handles array operations", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"tags" => ["production"]}})
      lql_string = "m.tags:@>production"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %FilterRule{
               path: "metadata.tags",
               operator: :list_includes,
               value: "production"
             } = rule
    end

    test "handles range operators", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"latency" => 100.0}})
      lql_string = "m.latency:100.0..500.0"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %FilterRule{
               path: "metadata.latency",
               operator: :range,
               values: [100.0, 500.0]
             } = rule
    end

    test "handles NULL values", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"optional_field" => "test"}})
      lql_string = "m.optional_field:NULL"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %FilterRule{
               path: "metadata.optional_field",
               operator: :=,
               value: :NULL
             } = rule
    end

    test "handles complex mixed query", %{source: source} do
      schema =
        build_schema(%{
          "metadata" => %{
            "status" => "error",
            "latency" => 200.0,
            "tags" => ["production", "api"],
            "user" => %{"id" => 123}
          }
        })

      lql_string =
        "m.status:error m.latency:>100 m.tags:@>production m.user.id:123 ~warning t:today"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      # Should be able to re-parse encoded string
      {:ok, reparsed_rules} = Parser.parse(encoded_string, schema)
      assert length(reparsed_rules) == length(parsed_rules)

      search_op =
        SearchOperation.new(%{
          source: source,
          querystring: lql_string,
          lql_rules: parsed_rules,
          tailing?: false,
          partition_by: :timestamp,
          type: :events
        })

      # Should separate different filter types
      assert length(search_op.lql_meta_and_msg_filters) == 5
      assert length(search_op.lql_ts_filters) == 1
      assert search_op.chart_rules == []
    end

    test "preserves field types through parse-encode cycle", %{source: _source} do
      schema =
        build_schema(%{
          "metadata" => %{
            "string_field" => "test",
            "int_field" => 42,
            "float_field" => 3.14,
            "bool_field" => true
          }
        })

      lql_string = "m.string_field:test m.int_field:42 m.float_field:3.14 m.bool_field:true"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      # Re-parse to verify types are preserved
      {:ok, reparsed_rules} = Parser.parse(encoded_string, schema)

      values = Enum.map(reparsed_rules, & &1.value)
      assert "test" in values
      assert 42 in values
      assert 3.14 in values
      assert true in values
    end
  end

  defp build_schema(input) do
    SchemaBuilder.build_table_schema(input, @default_schema)
  end
end
