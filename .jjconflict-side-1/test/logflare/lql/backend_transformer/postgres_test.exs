defmodule Logflare.Lql.BackendTransformer.PostgresTest do
  use Logflare.DataCase, async: true
  use ExUnitProperties

  import Ecto.Query

  alias Ecto.Query
  alias Ecto.Query.BooleanExpr
  alias Logflare.Lql.BackendTransformer.Postgres
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  describe "behaviour implementation" do
    test "implements all required callbacks" do
      assert function_exported?(Postgres, :transform_filter_rule, 2)
      assert function_exported?(Postgres, :transform_chart_rule, 5)
      assert function_exported?(Postgres, :transform_select_rule, 2)
      assert function_exported?(Postgres, :apply_filter_rules_to_query, 3)
      assert function_exported?(Postgres, :dialect, 0)
      assert function_exported?(Postgres, :quote_style, 0)
      assert function_exported?(Postgres, :validate_transformation_data, 1)
      assert function_exported?(Postgres, :build_transformation_data, 1)
      assert function_exported?(Postgres, :handle_nested_field_access, 2)
    end

    test "returns correct dialect and quote style" do
      assert Postgres.dialect() == "postgres"
      assert Postgres.quote_style() == "\""
    end
  end

  describe "validate_transformation_data/1" do
    test "validates valid transformation data" do
      assert Postgres.validate_transformation_data(%{schema: %{}}) == :ok
    end

    test "rejects invalid transformation data" do
      assert Postgres.validate_transformation_data(%{}) ==
               {:error, "Postgres transformer requires schema in transformation data"}
    end
  end

  describe "build_transformation_data/1" do
    test "passes through base data as-is" do
      data = %{test: "value"}
      assert Postgres.build_transformation_data(data) == data
    end
  end

  describe "transform_filter_rule/2" do
    test "transforms equality filter on top-level field" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) == ~s|dynamic([l], field(l, "event_message") == ^"error")|
    end

    test "transforms equality filter on nested JSONB field" do
      filter_rule =
        FilterRule.build(
          path: "m.status",
          operator: :=,
          value: "error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? = ?", ^"body->'metadata'->>'status'", ^"error"))|
    end

    test "transforms negated filter" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: "error",
          modifiers: %{negate: true}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? IS NULL", field(l, "event_message")) or not (field(l, "event_message") == ^"error"))|
    end

    test "transforms `= :NULL` into an IS NULL fragment" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: :NULL,
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? IS NULL", field(l, "event_message")))|
    end

    test "transforms `= :NULL` on a JSONB path into an IS NULL fragment" do
      filter_rule =
        FilterRule.build(
          path: "m.status",
          operator: :=,
          value: :NULL,
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? IS NULL", ^"body->'metadata'->>'status'"))|
    end

    test "transforms a negated comparison on a JSONB path" do
      filter_rule =
        FilterRule.build(
          path: "m.status",
          operator: :=,
          value: "ok",
          modifiers: %{negate: true}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], not fragment("? = ?", ^"body->'metadata'->>'status'", ^"ok"))|
    end

    test "transforms a negated `= :NULL` (special-cased to NOT IS NULL)" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :=,
          value: :NULL,
          modifiers: %{negate: true}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], not fragment("? IS NULL", field(l, "event_message")))|
    end

    test "treats a multi-segment path without `m.` prefix as a JSONB path" do
      filter_rule =
        FilterRule.build(
          path: "metadata.user.email",
          operator: :=,
          value: "a@b.c",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? = ?", ^"body->'metadata'->'user'->>'email'", ^"a@b.c"))|
    end

    test "transforms regex filter with PostgreSQL ~ operator" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :"~",
          value: "server.*error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? ~ ?", field(l, "event_message"), ^"server.*error"))|
    end

    test "transforms regex filter on JSONB field" do
      filter_rule =
        FilterRule.build(
          path: "m.message",
          operator: :"~",
          value: "server.*error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? ~ ?", ^"body->'metadata'->>'message'", ^"server.*error"))|
    end

    test "transforms string_contains filter" do
      filter_rule =
        FilterRule.build(
          path: "event_message",
          operator: :string_contains,
          value: "error",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? LIKE ?", field(l, "event_message"), ^"%error%"))|
    end

    test "transforms `string_contains` on a JSONB path" do
      filter_rule =
        FilterRule.build(
          path: "m.message",
          operator: :string_contains,
          value: "boom",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? LIKE ?", ^"body->'metadata'->>'message'", ^"%boom%"))|
    end

    test "transforms numeric comparison with type casting on JSONB" do
      filter_rule =
        FilterRule.build(
          path: "m.latency",
          operator: :>,
          value: 100,
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("(?)::numeric > ?", ^"body->'metadata'->>'latency'", ^100))|
    end

    test "transforms list_includes with JSONB @> operator" do
      filter_rule =
        FilterRule.build(
          path: "m.tags",
          operator: :list_includes,
          value: "production",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? @> ?::jsonb", ^"body->'metadata'->'tags'", ^"[\\"production\\"]"))|
    end

    test "transforms `list_includes` on a top-level array field" do
      filter_rule =
        FilterRule.build(
          path: "id",
          operator: :list_includes,
          value: "abc",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("? @> ?::jsonb", field(l, "id"), ^"[\\"abc\\"]"))|
    end

    test "transforms range operator on top-level field" do
      filter_rule =
        FilterRule.build(
          path: "timestamp",
          operator: :range,
          values: [~U[2024-01-01 00:00:00Z], ~U[2024-01-02 00:00:00Z]],
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment(\n  "? BETWEEN ? AND ?",\n  field(l, "timestamp"),\n  ^~U[2024-01-01 00:00:00Z],\n  ^~U[2024-01-02 00:00:00Z]\n))|
    end

    test "transforms range operator on JSONB field" do
      filter_rule =
        FilterRule.build(
          path: "m.count",
          operator: :range,
          values: [1, 100],
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment("(?)::numeric BETWEEN ? AND ?", ^"body->'metadata'->>'count'", ^1, ^100))|
    end

    test "transforms `list_includes_regexp` on a top-level array field" do
      filter_rule =
        FilterRule.build(
          path: "id",
          operator: :list_includes_regexp,
          value: "abc.*",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment(\n  "EXISTS(SELECT 1 FROM jsonb_array_elements_text(?) AS x WHERE x ~ ?)",\n  field(l, "id"),\n  ^"abc.*"\n))|
    end

    test "transforms `list_includes_regexp` on a JSONB array path" do
      filter_rule =
        FilterRule.build(
          path: "m.tags",
          operator: :list_includes_regexp,
          value: "prod-.*",
          modifiers: %{}
        )

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) ==
               ~s|dynamic([l], fragment(\n  "EXISTS(SELECT 1 FROM jsonb_array_elements_text(?) AS x WHERE x ~ ?)",\n  ^"body->'metadata'->'tags'",\n  ^"prod-.*"\n))|
    end

    test "treats a non-special single field name as a top-level field" do
      filter_rule =
        FilterRule.build(path: "level", operator: :=, value: "info", modifiers: %{})

      result = Postgres.transform_filter_rule(filter_rule, %{})

      assert inspect(result) == ~s|dynamic([l], field(l, "level") == ^"info")|
    end

    property "transforms ordering operators on the `timestamp` field for any DateTime value" do
      check all op <- member_of([:>, :>=, :<, :<=]),
                unix <- integer(0..4_102_444_800),
                dt = DateTime.from_unix!(unix) do
        filter_rule =
          FilterRule.build(
            path: "timestamp",
            operator: op,
            value: dt,
            modifiers: %{}
          )

        result = Postgres.transform_filter_rule(filter_rule, %{})

        assert inspect(result) ==
                 ~s|dynamic([l], field(l, "timestamp") #{Atom.to_string(op)} ^~U[#{DateTime.to_string(dt)}])|
      end
    end

    property "transforms numeric ordering operators on a JSONB path for any integer value" do
      check all op <- member_of([:>=, :<, :<=]),
                value <- integer() do
        filter_rule =
          FilterRule.build(
            path: "m.latency",
            operator: op,
            value: value,
            modifiers: %{}
          )

        result = Postgres.transform_filter_rule(filter_rule, %{})

        assert inspect(result) ==
                 ~s|dynamic([l], fragment("(?)::numeric #{Atom.to_string(op)} ?", ^"body->'metadata'->>'latency'", ^#{value}))|
      end
    end
  end

  describe "apply_filter_rules_to_query/3" do
    test "applies multiple filter rules" do
      query = from(l in "logs")

      rules = [
        FilterRule.build(
          path: "event_message",
          operator: :string_contains,
          value: "error",
          modifiers: %{}
        ),
        FilterRule.build(path: "m.status", operator: :=, value: 500, modifiers: %{})
      ]

      result = Postgres.apply_filter_rules_to_query(query, rules, [])

      assert %Ecto.Query{} = result
      assert length(result.wheres) == 2
    end

    test "returns query unchanged when no rules" do
      query = from(l in "logs")
      result = Postgres.apply_filter_rules_to_query(query, [], [])
      assert result == query
    end

    test "applies a JSONB range rule via where_match_filter_rule" do
      query = from(l in "logs")

      rule =
        FilterRule.build(path: "m.count", operator: :range, values: [1, 100], modifiers: %{})

      result = Postgres.apply_filter_rules_to_query(query, [rule])

      assert %Ecto.Query{wheres: [_]} = result
    end

    test "is callable without the trailing opts argument" do
      query = from(l in "logs")
      assert Postgres.apply_filter_rules_to_query(query, []) == query
    end
  end

  describe "transform_chart_rule/5" do
    setup do
      {:ok, query: from(l in "logs")}
    end

    @chart_aggregates [:count, :countd, :avg, :sum, :max, :p50, :p95, :p99]
    @chart_periods [:second, :minute, :hour, :day]

    for aggregate <- @chart_aggregates, period <- @chart_periods do
      test "generates #{aggregate} aggregation by #{period}", %{query: query} do
        result =
          Postgres.transform_chart_rule(
            query,
            unquote(aggregate),
            "m.latency",
            unquote(period),
            "timestamp"
          )

        assert %Ecto.Query{} = result
        assert result.group_bys != []
        assert result.order_bys != []
        assert result.select != nil
        assert_select_keys(result, [:timestamp, :count])
      end
    end
  end

  describe "transform_select_rule/2" do
    test "transforms wildcard select" do
      select_rule = %SelectRule{wildcard: true, path: "*"}
      result = Postgres.transform_select_rule(select_rule, %{})
      assert result == {:wildcard, []}
    end

    test "transforms top-level field select" do
      select_rule = %SelectRule{wildcard: false, path: "event_message"}
      result = Postgres.transform_select_rule(select_rule, %{})
      assert result == {:field, "event_message", []}
    end

    test "transforms nested field select" do
      select_rule = %SelectRule{wildcard: false, path: "m.status"}
      result = Postgres.transform_select_rule(select_rule, %{})
      assert result == {:nested_field, "m.status", []}
    end

    test "returns an error tuple for invalid select rules" do
      assert {:error, message} = Postgres.transform_select_rule(%{not: "valid"}, %{})
      assert message =~ "Invalid SelectRule"
    end
  end

  describe "apply_select_rules_to_query/3" do
    test "returns query unchanged for wildcard selection" do
      query = from(l in "logs")
      select_rules = [%SelectRule{wildcard: true, path: "*"}]

      result = Postgres.apply_select_rules_to_query(query, select_rules, [])
      assert result == query
    end

    test "returns query unchanged for empty select rules" do
      query = from(l in "logs")
      result = Postgres.apply_select_rules_to_query(query, [], [])
      assert result == query
    end

    test "is callable without the trailing opts argument" do
      query = from(l in "logs")
      assert Postgres.apply_select_rules_to_query(query, []) == query
    end

    test "builds combined select for specific fields" do
      query = from(l in "logs")

      select_rules = [
        %SelectRule{wildcard: false, path: "event_message"},
        %SelectRule{wildcard: false, path: "timestamp"}
      ]

      result = Postgres.apply_select_rules_to_query(query, select_rules, [])
      assert %Ecto.Query{} = result
      assert result.select != nil
    end
  end

  describe "handle_nested_field_access/2" do
    test "returns query unchanged (no joins needed for JSONB)" do
      query = from(l in "logs")
      result = Postgres.handle_nested_field_access(query, "m.status")
      assert result == query
    end
  end

  describe "apply_select_rules_to_query/3 with aliases" do
    test "applies top-level field with alias" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "event_message", alias: "msg"}

      result = Postgres.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert expr |> Macro.to_string() =~ "msg"
    end

    test "applies nested field with alias" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "metadata.user.id", alias: "user_id"}

      result = Postgres.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert expr |> Macro.to_string() =~ "user_id"
    end

    test "applies a nested JSONB field without an alias" do
      query = from(l in "logs")
      select_rule = %SelectRule{path: "m.status"}

      result = Postgres.apply_select_rules_to_query(query, [select_rule], [])

      assert %Ecto.Query{select: %{expr: expr}} = result
      assert Macro.to_string(expr) =~ "m.status"
    end
  end

  describe "where_timestamp_ago/4" do
    setup do
      {:ok, base_query: from("logs"), datetime: ~U[2025-02-21 03:27:12Z]}
    end

    @valid_intervals [
      {"MINUTE", 5, "minutes"},
      {"HOUR", 24, "hours"},
      {"DAY", 7, "days"},
      {"SECOND", 30, "seconds"},
      {"MILLISECOND", 1_000, "milliseconds"},
      {"MICROSECOND", 1_000_000, "microseconds"}
    ]

    for {unit, count, expected_suffix} <- @valid_intervals do
      test "builds correct interval fragment for #{unit}", %{
        base_query: base_query,
        datetime: datetime
      } do
        result = Postgres.where_timestamp_ago(base_query, datetime, unquote(count), unquote(unit))

        assert %Query{wheres: [%BooleanExpr{expr: expr} | _]} = result
        assert {:>=, _, [_field, {:fragment, _, fragment_parts}]} = expr

        raw_sql =
          fragment_parts
          |> Enum.filter(&match?({:raw, _}, &1))
          |> Enum.map(fn {:raw, s} -> s end)
          |> Enum.join()

        assert raw_sql =~ unquote(expected_suffix)
      end
    end

    test "composes with existing where clauses", %{datetime: datetime} do
      assert %Query{} =
               query =
               from("logs")
               |> where([t], t.level == "error")
               |> Postgres.where_timestamp_ago(datetime, 10, "MINUTE")
               |> where([t], t.status == 500)

      assert length(query.wheres) == 3
    end

    test "raises ArgumentError for invalid interval", %{
      base_query: base_query,
      datetime: datetime
    } do
      assert_raise ArgumentError, "Invalid interval: INVALID", fn ->
        Postgres.where_timestamp_ago(base_query, datetime, 1, "INVALID")
      end
    end
  end

  defp assert_select_keys(%Query{select: select}, expected_keys) do
    {:%{}, [], fields} = select.expr
    keys = Enum.map(fields, fn {key, _} -> key end) |> Enum.sort()
    assert keys == Enum.sort(expected_keys)
  end
end
