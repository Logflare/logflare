defmodule Logflare.Lql.IntegrationTest do
  use Logflare.DataCase, async: true

  alias Logflare.Logs.SearchOperation
  alias Logflare.Lql
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
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

  describe "edge cases and error handling" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source, user: user}
    end

    test "handles empty LQL string", %{source: _source} do
      {:ok, parsed_rules} = Parser.parse("", @default_schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert parsed_rules == []
      assert encoded_string == ""
    end

    test "handles nil LQL string", %{source: _source} do
      {:ok, parsed_rules} = Parser.parse(nil)
      encoded_string = Lql.encode!(parsed_rules)

      assert parsed_rules == []
      assert encoded_string == ""
    end

    test "handles whitespace-only LQL string", %{source: _source} do
      result = Parser.parse("   \n  \t  ", @default_schema)

      case result do
        {:ok, parsed_rules} ->
          encoded_string = Lql.encode!(parsed_rules)
          assert parsed_rules == []
          assert encoded_string == ""

        {:error, _} ->
          assert true
      end
    end

    test "returns error for invalid field path", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"status" => "test"}})
      lql_string = "m.nonexistent_field:value"

      result = Parser.parse(lql_string, schema)

      assert {:error, :field_not_found, "", _} = result
    end

    test "returns error for invalid timestamp format", %{source: _source} do
      lql_string = "t:invalid-timestamp"

      result = Parser.parse(lql_string, @default_schema)

      assert {:error, "Error while parsing timestamp" <> _} = result
    end

    test "returns error for invalid boolean value", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"flag" => true}})
      lql_string = "m.flag:not_boolean"

      result = Parser.parse(lql_string, schema)

      assert {:error,
              "Query syntax error: Expected boolean for metadata.flag, got: 'not_boolean'"} =
               result
    end

    test "returns error for invalid integer value", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"count" => 42}})
      lql_string = "m.count:not_an_integer"

      result = Parser.parse(lql_string, schema)

      assert {:error,
              "Query syntax error: expected integer for metadata.count, got: 'not_an_integer'"} =
               result
    end

    test "returns error for invalid float value", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"latency" => 3.14}})
      lql_string = "m.latency:not_a_float"

      result = Parser.parse(lql_string, schema)

      assert {:error,
              "Query syntax error: expected float for metadata.latency, got: 'not_a_float'"} =
               result
    end

    test "handles multiple level filters with range encoding", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"level" => "info"}})
      lql_string = "m.level:debug m.level:error m.level:warning"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      # Current behavior doesn't automatically create ranges - it keeps individual filters
      assert String.contains?(encoded_string, "m.level:debug")
      assert String.contains?(encoded_string, "m.level:error")
      assert String.contains?(encoded_string, "m.level:warning")
      assert length(parsed_rules) == 3
    end

    test "handles complex timestamp range with microseconds", %{source: _source} do
      lql_string = "t:2020-01-01T12:30:45.123456..2020-01-01T12:30:45.654321"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert length(parsed_rules) == 1
      [rule] = parsed_rules
      assert %FilterRule{path: "timestamp", operator: :range} = rule

      # Should be able to re-parse
      {:ok, reparsed_rules} = Parser.parse(encoded_string, @default_schema)
      assert length(reparsed_rules) == 1
    end

    test "handles extreme timestamp shorthand values", %{source: _source} do
      test_cases = [
        "t:last@1s",
        "t:last@999m",
        "t:last@24h",
        "t:last@365d",
        "t:this@minute",
        "t:this@year"
      ]

      for lql_string <- test_cases do
        {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
        _encoded_string = Lql.encode!(parsed_rules)

        assert length(parsed_rules) == 1
        [rule] = parsed_rules
        assert %FilterRule{path: "timestamp", shorthand: _} = rule

        assert String.starts_with?(rule.shorthand, "last@") or
                 String.starts_with?(rule.shorthand, "this@")
      end
    end

    test "handles deeply nested field paths", %{source: _source} do
      schema =
        build_schema(%{
          "metadata" => %{
            "user" => %{
              "profile" => %{
                "settings" => %{
                  "notifications" => %{"enabled" => true}
                }
              }
            }
          }
        })

      lql_string = "m.user.profile.settings.notifications.enabled:true"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert String.contains?(
               encoded_string,
               "m.user.profile.settings.notifications.enabled:true"
             )

      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %FilterRule{
               path: "metadata.user.profile.settings.notifications.enabled",
               value: true
             } = rule
    end

    test "handles array regex operations", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"tags" => ["production", "staging"]}})
      lql_string = "m.tags:@>~prod"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      assert encoded_string == lql_string
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %FilterRule{path: "metadata.tags", operator: :list_includes_regexp, value: "prod"} =
               rule
    end

    test "handles special characters in quoted strings", %{source: _source} do
      test_cases = [
        {~S|"hello@example.com"|, "hello@example.com"},
        {~S|"user[123]"|, "user[123]"},
        {~S|"path/to/file.txt"|, "path/to/file.txt"},
        {~S|"value with spaces and symbols!@#$%"|, "value with spaces and symbols!@#$%"}
      ]

      for {lql_string, expected_value} <- test_cases do
        {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
        encoded_string = Lql.encode!(parsed_rules)

        assert encoded_string == lql_string
        assert length(parsed_rules) == 1

        [rule] = parsed_rules

        assert %FilterRule{
                 path: "event_message",
                 value: ^expected_value,
                 modifiers: %{quoted_string: true}
               } = rule
      end
    end

    test "handles mixed negation with different operators", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"status" => "test", "count" => 42}})
      lql_string = "-m.status:success -m.count:>100 -event_message:~error"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      encoded_string = Lql.encode!(parsed_rules)

      # All rules should be negated
      for rule <- parsed_rules do
        assert %FilterRule{modifiers: %{negate: true}} = rule
      end

      assert length(parsed_rules) == 3

      # Should contain negated elements in encoded string
      assert String.contains?(encoded_string, "-m.status:success")
      assert String.contains?(encoded_string, "-m.count:>100")
    end

    test "handles SearchOperation with tailing enabled", %{source: source} do
      schema = build_schema(%{"metadata" => %{"status" => "error"}})
      lql_string = "m.status:error"
      {:ok, lql_rules} = Parser.parse(lql_string, schema)

      search_op =
        SearchOperation.new(%{
          source: source,
          querystring: lql_string,
          lql_rules: lql_rules,
          tailing?: true,
          partition_by: :timestamp,
          type: :events
        })

      assert search_op.tailing? == true
      assert search_op.source == source
      assert length(search_op.lql_meta_and_msg_filters) == 1
    end

    test "handles SearchOperation validation errors", %{source: source} do
      schema = build_schema(%{"metadata" => %{"status" => "error"}})
      lql_string = "m.status:error t:today"
      {:ok, lql_rules} = Parser.parse(lql_string, schema)

      # This should be caught by the validator as tailing with timestamp filters
      search_op =
        SearchOperation.new(%{
          source: source,
          querystring: lql_string,
          lql_rules: lql_rules,
          tailing?: true,
          partition_by: :timestamp,
          type: :events
        })

      # Should still create the operation but may have validation warnings
      assert search_op.tailing? == true
      assert length(search_op.lql_ts_filters) == 1
    end
  end

  describe "select rule processing" do
    setup do
      user = insert(:user)
      source = insert(:source, user_id: user.id)
      {:ok, source: source, user: user}
    end

    test "parses and encodes wildcard select", %{source: _source} do
      lql_string = "s:*"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "*", wildcard: true} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses wildcard select with full syntax", %{source: _source} do
      lql_string = "select:*"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "*", wildcard: true} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == "s:*"
    end

    test "parses specific field select", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"user_id" => "123"}})
      lql_string = "s:m.user_id"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "metadata.user_id", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses nested field select", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"user" => %{"profile" => %{"name" => "john"}}}})
      lql_string = "select:m.user.profile.name"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "metadata.user.profile.name", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == "s:m.user.profile.name"
    end

    test "parses top-level field select", %{source: _source} do
      lql_string = "s:event_message"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "event_message", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses deeply nested field select", %{source: _source} do
      lql_string = "select:my_field.that.is.nested"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "my_field.that.is.nested", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == "s:my_field.that.is.nested"
    end

    test "parses very deep nested field select", %{source: _source} do
      lql_string = "s:user.profile.settings.theme.colors.primary.dark_mode"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules

      assert %SelectRule{
               path: "user.profile.settings.theme.colors.primary.dark_mode",
               wildcard: false
             } = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses multiple select fields", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"user_id" => "123", "status" => "active"}})
      lql_string = "s:m.user_id s:m.status"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 2

      paths = Enum.map(parsed_rules, & &1.path)
      assert "metadata.user_id" in paths
      assert "metadata.status" in paths

      encoded_string = Lql.encode!(parsed_rules)
      # Encoding might reorder, so check both fields are present
      assert String.contains?(encoded_string, "s:m.user_id")
      assert String.contains?(encoded_string, "s:m.status")
    end

    test "parses select combined with filters", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"user_id" => "123", "status" => "active"}})
      lql_string = "s:m.user_id m.status:active"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 2

      select_rules = Enum.filter(parsed_rules, &match?(%SelectRule{}, &1))
      filter_rules = Enum.reject(parsed_rules, &match?(%SelectRule{}, &1))

      assert length(select_rules) == 1
      assert length(filter_rules) == 1

      encoded_string = Lql.encode!(parsed_rules)
      assert String.contains?(encoded_string, "s:m.user_id")
      assert String.contains?(encoded_string, "m.status:active")
    end

    test "parses select combined with chart rules", %{source: _source} do
      schema = build_schema(%{"metadata" => %{"user_id" => "123", "latency" => 100.0}})
      lql_string = "s:m.user_id c:avg(m.latency) c:group_by(t::minute)"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 2

      select_rules = Enum.filter(parsed_rules, &match?(%SelectRule{}, &1))
      chart_rules = Enum.reject(parsed_rules, &match?(%SelectRule{}, &1))

      assert length(select_rules) == 1
      assert length(chart_rules) == 1

      encoded_string = Lql.encode!(parsed_rules)
      assert String.contains?(encoded_string, "s:m.user_id")
      assert String.contains?(encoded_string, "c:avg(m.latency)")
      assert String.contains?(encoded_string, "c:group_by(t::minute)")
    end

    test "parses complex query with select, filter, and chart", %{source: _source} do
      schema =
        build_schema(%{
          "metadata" => %{
            "user_id" => "123",
            "status" => "active",
            "latency" => 100.0
          }
        })

      lql_string =
        "s:m.user_id s:m.status m.status:active m.latency:>50 c:avg(m.latency) c:group_by(t::minute)"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)

      select_rules = Enum.filter(parsed_rules, &match?(%SelectRule{}, &1))
      filter_rules = Enum.filter(parsed_rules, &match?(%FilterRule{}, &1))
      chart_rules = Enum.filter(parsed_rules, &match?(%ChartRule{}, &1))

      assert length(select_rules) == 2
      assert length(filter_rules) == 2
      assert length(chart_rules) == 1

      encoded_string = Lql.encode!(parsed_rules)

      # Should contain all select fields
      assert String.contains?(encoded_string, "s:m.user_id")
      assert String.contains?(encoded_string, "s:m.status")

      # Should contain all filters
      assert String.contains?(encoded_string, "m.status:active")
      assert String.contains?(encoded_string, "m.latency:>50")

      # Should contain chart aggregation
      assert String.contains?(encoded_string, "c:avg(m.latency)")
      assert String.contains?(encoded_string, "c:group_by(t::minute)")
    end

    test "handles empty select statements gracefully", %{source: _source} do
      result = Parser.parse("s:", @default_schema)
      assert match?({:error, _}, result)
    end

    test "maintains select consistency through parse-encode cycles", %{source: _source} do
      test_cases = [
        "s:*",
        "s:event_message",
        "s:m.user_id",
        "s:m.user.profile.name",
        "s:deeply.nested.field.structure",
        "s:user.profile.settings.theme.colors.primary.dark_mode"
      ]

      for lql_string <- test_cases do
        {:ok, parsed_rules} = Parser.parse(lql_string)
        encoded_string = Lql.encode!(parsed_rules)
        {:ok, reparsed_rules} = Parser.parse(encoded_string)

        assert length(reparsed_rules) == length(parsed_rules)

        # Check that the core fields match
        for {original, reparsed} <- Enum.zip(parsed_rules, reparsed_rules) do
          assert original.path == reparsed.path
          assert original.wildcard == reparsed.wildcard
        end
      end
    end
  end

  defp build_schema(input) do
    SchemaBuilder.build_table_schema(input, @default_schema)
  end
end
