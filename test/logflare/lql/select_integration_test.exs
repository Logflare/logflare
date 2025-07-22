defmodule Logflare.Lql.SelectIntegrationTest do
  use Logflare.DataCase, async: true

  alias Logflare.Lql
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules.SelectRule
  alias Logflare.Source.BigQuery.SchemaBuilder

  @default_schema SchemaBuilder.initial_table_schema()

  describe "select parsing and encoding" do
    test "parses wildcard select" do
      lql_string = "s:*"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "*", wildcard: true} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses wildcard select with full syntax" do
      lql_string = "select:*"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "*", wildcard: true} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == "s:*"
    end

    test "parses specific field select" do
      schema = build_schema(%{"metadata" => %{"user_id" => "123"}})
      lql_string = "s:m.user_id"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "metadata.user_id", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses nested field select" do
      schema = build_schema(%{"metadata" => %{"user" => %{"profile" => %{"name" => "john"}}}})
      lql_string = "select:m.user.profile.name"

      {:ok, parsed_rules} = Parser.parse(lql_string, schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "metadata.user.profile.name", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == "s:m.user.profile.name"
    end

    test "parses top-level field select" do
      lql_string = "s:event_message"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "event_message", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == lql_string
    end

    test "parses deeply nested field select" do
      lql_string = "select:my_field.that.is.nested"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "my_field.that.is.nested", wildcard: false} = rule

      encoded_string = Lql.encode!(parsed_rules)
      assert encoded_string == "s:my_field.that.is.nested"
    end

    test "parses very deep nested field select" do
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

    test "parses mixed nested field types" do
      lql_string = "s:user.id s:user.profile.name s:metadata.request.headers.authorization"

      {:ok, parsed_rules} = Parser.parse(lql_string, @default_schema)
      assert length(parsed_rules) == 3

      paths = Enum.map(parsed_rules, & &1.path)
      assert "user.id" in paths
      assert "user.profile.name" in paths
      assert "metadata.request.headers.authorization" in paths

      encoded_string = Lql.encode!(parsed_rules)
      # All paths should be present in encoded form
      assert String.contains?(encoded_string, "s:user.id")
      assert String.contains?(encoded_string, "s:user.profile.name")
      assert String.contains?(encoded_string, "s:m.request.headers.authorization")
    end

    test "parses multiple select fields" do
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

    test "parses select combined with filters" do
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

    test "parses select combined with chart rules" do
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

    test "parses complex query with select, filter, and chart" do
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
      filter_rules = Enum.filter(parsed_rules, &match?(%Logflare.Lql.Rules.FilterRule{}, &1))
      chart_rules = Enum.filter(parsed_rules, &match?(%Logflare.Lql.Rules.ChartRule{}, &1))

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
  end

  describe "error handling" do
    test "handles empty select statements gracefully" do
      result = Parser.parse("s:", @default_schema)
      assert match?({:error, _}, result)
    end

    test "parses without schema validation" do
      # Should work without schema for basic cases
      {:ok, parsed_rules} = Parser.parse("s:*")
      assert length(parsed_rules) == 1

      [rule] = parsed_rules
      assert %SelectRule{path: "*", wildcard: true} = rule
    end
  end

  describe "round-trip consistency" do
    test "maintains consistency through parse-encode cycles" do
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
