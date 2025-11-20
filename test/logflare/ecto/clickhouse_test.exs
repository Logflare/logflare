defmodule Logflare.Ecto.ClickHouseTest do
  use ExUnit.Case, async: true

  import Ecto.Query

  alias Logflare.Ecto.ClickHouse

  describe "to_sql/1" do
    test "converts simple SELECT query to ClickHouse SQL" do
      query = from(t in "logs", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert is_binary(sql)
      assert String.contains?(sql, "SELECT")
      assert String.contains?(sql, ~s("logs"))
      assert is_list(params)
    end

    test "converts SELECT with WHERE clause" do
      query = from(t in "logs", where: t.level == "error", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "WHERE")
      assert String.contains?(sql, "=")
      assert params == ["error"]
    end

    test "converts SELECT with field selection" do
      query = from(t in "logs", select: %{message: t.event_message, level: t.level})

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "SELECT")
      assert String.contains?(sql, ~s("event_message"))
      assert String.contains?(sql, ~s("level"))
    end

    test "converts query with ORDER BY" do
      query = from(t in "logs", order_by: [desc: t.timestamp], select: t)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "ORDER BY")
      assert String.contains?(sql, "DESC")
    end

    test "converts query with LIMIT" do
      query = from(t in "logs", limit: 100, select: t)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "LIMIT")
      assert String.contains?(sql, "100")
    end

    test "converts query with OFFSET" do
      query = from(t in "logs", offset: 50, select: t)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "OFFSET")
      assert String.contains?(sql, "50")
    end

    test "converts query with GROUP BY" do
      query = from(t in "logs", group_by: t.level, select: {t.level, count(t.id)})

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "GROUP BY")
    end

    test "converts query with JOIN" do
      query =
        from(l in "logs",
          join: u in "users",
          on: l.user_id == u.id,
          select: {l.event_message, u.name}
        )

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "JOIN")
      assert String.contains?(sql, "ON")
    end

    test "handles comparison operators" do
      query = from(t in "logs", where: t.level > 3, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ">")
      assert params == [3]
    end

    test "handles IN operator with list" do
      query = from(t in "logs", where: t.level in ["error", "warn"], select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "IN")
      assert params == ["error", "warn"]
    end

    test "handles NULL checks with IS NULL" do
      query = from(t in "logs", where: is_nil(t.optional_field), select: t)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "isNull")
    end

    test "handles NOT NULL checks with IS NOT NULL" do
      query = from(t in "logs", where: not is_nil(t.required_field), select: t)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "isNotNull")
    end

    test "handles aggregate functions" do
      query = from(t in "logs", select: %{total: count(t.id), avg_size: avg(t.size)})

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "count")
      assert String.contains?(sql, "avg")
    end

    test "handles DISTINCT" do
      query = from(t in "logs", distinct: true, select: t.level)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "DISTINCT")
    end

    test "handles subqueries" do
      subquery = from(t in "logs", where: t.level == "error", select: t.id)
      query = from(t in "logs", where: t.id in subquery(subquery), select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "IN")
      assert String.contains?(sql, "(")
      assert params == ["error"]
    end

    test "converts query with CTE (WITH clause)" do
      cte_query = from(t in "logs", where: t.level == "error")

      query =
        from(t in "logs")
        |> with_cte("error_logs", as: ^cte_query)
        |> join(:inner, [t], e in "error_logs", on: t.id == e.id)
        |> select([t], t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "WITH")
      assert params == ["error"]
    end
  end

  describe "to_sql/1 with field()" do
    test "handles field() with string field name outside fragments" do
      field_name = "event_message"
      query = from(t in "logs", where: field(t, ^field_name) == "error", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("event_message"))
      assert params == ["error"]
      # Field name should NOT be in params
      refute "event_message" in params
    end

    test "handles field() with string field name in SELECT" do
      field_name = "level"
      query = from(t in "logs", select: %{value: field(t, ^field_name)})

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("level"))
      assert params == []
      # Field name should NOT be in params
      refute "level" in params
    end

    test "handles field() inside fragment()" do
      field_name = "event_message"
      pattern = "server.*error"
      query = from(t in "logs", where: fragment("match(?, ?)", field(t, ^field_name), ^pattern))

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("event_message"))
      assert String.contains?(sql, "match")

      # Only the pattern should be a parameter, NOT the field name
      assert params == [pattern]
      refute "event_message" in params

      # Verify the field is NOT parameterized (should not see parameter before the comma)
      refute String.contains?(sql, ~s({$0:String}, {$1:String}))
      refute String.contains?(sql, ~s("event_message", "event_message"))
    end

    test "handles multiple field() expressions in fragment()" do
      field1 = "source_field"
      field2 = "target_field"

      query =
        from(t in "logs",
          where: fragment("? = ?", field(t, ^field1), field(t, ^field2)),
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)

      # Both fields should be SQL identifiers
      assert String.contains?(sql, ~s("source_field"))
      assert String.contains?(sql, ~s("target_field"))

      # Neither field name should be in params
      assert params == []
      refute "source_field" in params
      refute "target_field" in params
    end

    test "handles field() with value parameter in fragment()" do
      field_name = "status_code"
      value = 404

      query =
        from(t in "logs",
          where: fragment("? > ?", field(t, ^field_name), ^value),
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)

      # Field should be identifier, value should be parameter
      assert String.contains?(sql, ~s("status_code"))
      assert params == [value]
      refute "status_code" in params
    end

    test "regex operator with field() in fragment()" do
      # This is the pattern used in LQL ClickHouse transformer
      field_path = "event_message"
      regex_pattern = "error|warning"

      query =
        from(l in "logs",
          where: fragment("match(?, ?)", field(l, ^field_path), ^regex_pattern),
          select: l
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)

      # Should generate: match(l0."event_message", {$0:String})
      assert String.contains?(sql, "match")
      assert String.contains?(sql, ~s("event_message"))

      # Only the regex pattern should be a parameter
      assert params == [regex_pattern]
      refute "event_message" in params

      # Verify valid SQL structure
      refute String.contains?(sql, ~s("event_message".{$))
      refute String.contains?(sql, "{$0:String}.{$1:String}")
    end

    test "position() with field() - string_contains operator" do
      field_path = "log_message"
      search_value = "timeout"

      query =
        from(l in "logs",
          where: fragment("position(?, ?) > 0", field(l, ^field_path), ^search_value),
          select: l
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "position")
      assert String.contains?(sql, ~s("log_message"))
      assert params == [search_value]
      refute "log_message" in params
    end

    test "has() with field() - list_includes operator" do
      field_path = "tags"
      search_value = "production"

      query =
        from(l in "logs",
          where: fragment("has(?, ?)", field(l, ^field_path), ^search_value),
          select: l
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "has")
      assert String.contains?(sql, ~s("tags"))
      assert params == [search_value]
      refute "tags" in params
    end

    test "arrayExists with field() - list_includes_regexp operator" do
      field_path = "labels"
      regex_pattern = "prod.*"

      query =
        from(l in "logs",
          where:
            fragment("arrayExists(x -> match(x, ?), ?)", ^regex_pattern, field(l, ^field_path)),
          select: l
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "arrayExists")
      assert String.contains?(sql, ~s("labels"))
      assert params == [regex_pattern]
      refute "labels" in params
    end

    test "BETWEEN with field() - range operator" do
      field_path = "response_time"
      min_value = 100
      max_value = 500

      query =
        from(l in "logs",
          where: fragment("? BETWEEN ? AND ?", field(l, ^field_path), ^min_value, ^max_value),
          select: l
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "BETWEEN")
      assert String.contains?(sql, ~s("response_time"))
      assert params == [min_value, max_value]
      refute "response_time" in params
    end
  end

  describe "to_sql/1 with sandboxed LQL queries" do
    test "handles simple sandboxed select query" do
      # Simulates: from(t in "my_cte", select: %{"field1" => field(t, ^"field1")})
      query = from(t in "my_cte", select: %{"field1" => field(t, ^"field1")})

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("my_cte"))
      assert String.contains?(sql, ~s("field1"))
      assert params == []
    end

    test "handles sandboxed query with filter using field()" do
      # Simulates: from(t in "my_cte", where: field(t, ^"status") == ^"error")
      query = from(t in "my_cte", where: field(t, ^"status") == ^"error", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("status"))
      assert params == ["error"]
      refute "status" in params
    end

    test "handles sandboxed query with regex filter" do
      # Simulates LQL: ~pattern on sandboxed query
      query =
        from(t in "my_cte",
          where: fragment("match(?, ?)", field(t, ^"event_message"), ^"server.*"),
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "match")
      assert String.contains?(sql, ~s("event_message"))
      assert params == ["server.*"]
    end
  end

  describe "to_sql/1 error handling" do
    test "returns error for query with locks" do
      query = from(t in "logs", lock: "FOR UPDATE", select: t)

      assert {:error, message} = ClickHouse.to_sql(query)
      assert message == "ClickHouse does not support locks"
    end

    test "handles invalid query gracefully" do
      # Create an invalid query structure
      query = %Ecto.Query{}

      assert {:error, _message} = ClickHouse.to_sql(query)
    end
  end

  describe "parameter type handling" do
    test "generates correct parameter types for strings" do
      query = from(t in "logs", where: t.message == ^"test", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "{$0:String}")
      assert params == ["test"]
    end

    test "generates correct parameter types for integers" do
      query = from(t in "logs", where: t.count == ^42, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "{$0:Int64}")
      assert params == [42]
    end

    test "generates correct parameter types for booleans" do
      query = from(t in "logs", where: t.active == ^true, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "{$0:Bool}")
      assert params == [true]
    end

    test "generates correct parameter types for DateTime" do
      dt = ~U[2024-01-01 12:00:00Z]
      query = from(t in "logs", where: t.created_at > ^dt, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "{$0:DateTime64")
      assert params == [dt]
    end

    test "generates correct parameter types for Date" do
      date = ~D[2024-01-01]
      query = from(t in "logs", where: t.log_date == ^date, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "{$0:Date}")
      assert params == [date]
    end

    test "generates correct parameter types for arrays" do
      tags = ["error", "warning"]
      query = from(t in "logs", where: t.tag in ^tags, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, "{$0:String}")
      assert String.contains?(sql, "{$1:String}")
      assert params == tags
    end
  end

  describe "identifier quoting" do
    test "quotes table names with double quotes" do
      query = from(t in "my_table", select: t)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("my_table"))
    end

    test "quotes field names with double quotes" do
      query = from(t in "logs", select: t.event_message)

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("event_message"))
    end

    test "quotes dynamic field names with double quotes" do
      field_name = "custom_field"
      query = from(t in "logs", select: field(t, ^field_name))

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)
      assert String.contains?(sql, ~s("custom_field"))
    end
  end

  describe "SQL generation correctness" do
    test "maintains correct parameter ordering" do
      query =
        from(t in "logs",
          where: t.level == ^"error" and t.code > ^500,
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)
      assert params == ["error", 500]
      assert String.contains?(sql, "{$0:String}")
      assert String.contains?(sql, "{$1:Int64}")
    end

    test "does not duplicate parameters" do
      value = "test"
      query = from(t in "logs", where: t.field1 == ^value or t.field2 == ^value, select: t)

      assert {:ok, {_sql, params}} = ClickHouse.to_sql(query)

      # Should have two separate parameter references
      assert params == [value, value]
    end

    test "correctly remaps parameters with limit and offset" do
      query =
        from(t in "logs",
          where: t.level == ^"error" and t.code > ^500,
          limit: ^10,
          offset: ^5,
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query)

      assert params == ["error", 500, 10, 5]
      assert String.contains?(sql, "{$0:String}")
      assert String.contains?(sql, "{$1:Int64}")
      assert String.contains?(sql, "LIMIT {$2:Int64}")
      assert String.contains?(sql, "OFFSET {$3:Int64}")
    end
  end

  describe "map select with fragments containing AS aliases" do
    test "does not duplicate AS when fragment already has alias" do
      query =
        from(t in "logs",
          select: %{
            value: fragment("COUNT(?) as value", t.timestamp)
          }
        )

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)

      # Should have only one AS (from the fragment), not a duplicate
      refute String.contains?(sql, "as value AS")
      assert String.contains?(sql, "COUNT")
      assert String.contains?(sql, "as value")
    end

    test "does not duplicate AS for aggregation with existing alias" do
      query =
        from(t in "logs",
          select: %{
            timestamp: fragment("TIMESTAMP_TRUNC(?, DAY) as timestamp", t.timestamp),
            count: fragment("COUNT(*) as count")
          }
        )

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)

      # Should not have duplicate AS clauses
      refute String.contains?(sql, "as timestamp AS")
      refute String.contains?(sql, "as count AS")
      assert String.contains?(sql, "TIMESTAMP_TRUNC")
      assert String.contains?(sql, "COUNT")
    end

    test "adds AS when fragment does not have alias" do
      query =
        from(t in "logs",
          select: %{
            value: fragment("COUNT(?)", t.timestamp)
          }
        )

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)

      # Should add AS "value" since fragment doesn't have it
      assert String.contains?(sql, ~s(AS "value"))
      assert String.contains?(sql, "COUNT")
    end
  end

  describe "list select handling" do
    test "converts list select to multiple columns, not array literal" do
      query = from(t in "logs", select: [t.timestamp, t.id, t.event_message])

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)

      # Should be comma-separated columns
      assert String.contains?(sql, ~s("timestamp"))
      assert String.contains?(sql, ~s("id"))
      assert String.contains?(sql, ~s("event_message"))

      # Should NOT be wrapped in array brackets [...]
      # The SELECT clause should have: field1, field2, field3
      # Not: [field1, field2, field3]
      refute Regex.match?(~r/SELECT\s+\[.*"timestamp".*,.*"id".*,.*"event_message".*\]/, sql)
    end

    test "list select with mixed field types" do
      query = from(t in "logs", select: [t.timestamp, t.id, t.event_message, t.metadata])

      assert {:ok, {sql, _params}} = ClickHouse.to_sql(query)

      # All fields should be comma-separated
      assert String.contains?(sql, ~s("timestamp"))
      assert String.contains?(sql, ~s("id"))
      assert String.contains?(sql, ~s("event_message"))
      assert String.contains?(sql, ~s("metadata"))
    end
  end

  describe "to_sql/2 with inline_params option" do
    test "inlines string parameters when inline_params: true" do
      query = from(t in "logs", where: t.message == ^"test", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)

      assert params == []
      assert String.contains?(sql, "'test'")
      refute String.contains?(sql, "{$")
    end

    test "inlines integer parameters when inline_params: true" do
      query = from(t in "logs", where: t.count == ^42, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)
      assert params == []
      assert String.contains?(sql, "42")
      refute String.contains?(sql, "{$")
    end

    test "inlines boolean parameters when inline_params: true" do
      query = from(t in "logs", where: t.active == ^true, select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)
      assert params == []

      # Boolean true should be inlined as 'true'
      assert String.contains?(sql, "true")
      refute String.contains?(sql, "{$")
    end

    test "inlines regex pattern parameters when inline_params: true" do
      query =
        from(t in "logs",
          where: fragment("match(?, ?)", field(t, ^"event_message"), ^"error.*timeout"),
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)
      assert params == []
      assert String.contains?(sql, "'error.*timeout'")
      refute String.contains?(sql, "{$")
    end

    test "inlines multiple parameters when inline_params: true" do
      query =
        from(t in "logs",
          where: t.level == ^"error" and t.code > ^500,
          select: t
        )

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)
      assert params == []
      assert String.contains?(sql, "'error'")
      assert String.contains?(sql, "500")
      refute String.contains?(sql, "{$")
    end

    test "handles empty params list when inline_params: true" do
      query = from(t in "logs", where: t.level == "error", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)
      assert params == []
      refute String.contains?(sql, "{$")
    end

    test "uses parameter placeholders when inline_params: false (default)" do
      query = from(t in "logs", where: t.message == ^"test", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: false)
      assert params == ["test"]
      assert String.contains?(sql, "{$0:String}")
      refute String.contains?(sql, "'test'")
    end

    test "escapes single quotes in inlined string parameters" do
      query = from(t in "logs", where: t.message == ^"test's message", select: t)

      assert {:ok, {sql, params}} = ClickHouse.to_sql(query, inline_params: true)
      assert params == []

      # Single quotes should be escaped by doubling them
      assert String.contains?(sql, "'test''s message'")
      refute String.contains?(sql, "{$")
    end
  end
end
