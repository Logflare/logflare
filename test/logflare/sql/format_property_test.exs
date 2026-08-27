defmodule Logflare.Sql.FormatPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Sql
  alias Logflare.Sql.Parser

  @max_runs 25

  property "formatting produces valid BigQuery queries" do
    check all values <- query_values(), max_runs: @max_runs do
      Enum.each(bigquery_cases(values), &assert_valid_formatting(:bq_sql, &1))
    end
  end

  property "formatting produces valid ClickHouse queries" do
    check all values <- query_values(), max_runs: @max_runs do
      Enum.each(clickhouse_cases(values), &assert_valid_formatting(:ch_sql, &1))
    end
  end

  property "formatting produces valid PostgreSQL queries" do
    check all values <- query_values(), max_runs: @max_runs do
      Enum.each(postgres_cases(values), &assert_valid_formatting(:pg_sql, &1))
    end
  end

  defp assert_valid_formatting(
         language,
         %{family: family, sql: sql, adjacent_brackets: adjacent_brackets}
       ) do
    dialect = Sql.to_dialect(language)
    failure_context = "dialect: #{dialect}\nfamily: #{family}\ninput: #{sql}"

    assert {:ok, _} = Parser.parse(dialect, sql), failure_context
    assert {:ok, formatted} = Sql.format(sql), failure_context

    formatted_failure_context = "#{failure_context}\nformatted: #{formatted}"

    assert {:ok, _} = Parser.parse(dialect, formatted), formatted_failure_context

    Enum.each(adjacent_brackets, fn expression ->
      assert formatted =~ expression <> "[",
             "expression-bracket whitespace changed\n#{formatted_failure_context}\nexpression: #{expression}"
    end)
  end

  defp bigquery_cases(values) do
    common_cases(values) ++
      [
        query_case(
          :quoted_table,
          "SELECT #{values.column} FROM `#{values.project}.#{values.dataset}.#{values.table}`"
        ),
        query_case(
          :struct,
          "SELECT struct(#{values.number} AS field_number, '#{values.text}' AS field_text) AS record_value"
        ),
        query_case(
          :array,
          "SELECT array[#{values.number}, #{values.other_number}] AS array_value",
          ["array"]
        ),
        query_case(
          :unnest,
          "SELECT item FROM unnest([#{values.number}, #{values.other_number}]) AS item"
        ),
        query_case(
          :offset_subscript,
          "SELECT #{values.column}[offset(#{values.index})] FROM #{values.table}",
          [values.column]
        ),
        query_case(:named_parameter, "SELECT @query_parameter AS parameter_value")
      ]
  end

  defp clickhouse_cases(values) do
    common_cases(values) ++
      [
        query_case(
          :map_access,
          "SELECT #{values.column}['#{values.map_key}'] FROM #{values.table}",
          [values.column]
        ),
        query_case(
          :array_subscript,
          "SELECT #{values.column}[#{values.index}] FROM #{values.table}",
          [values.column]
        ),
        query_case(
          :tuple_and_map,
          "SELECT tuple(#{values.number}, '#{values.text}'), map('#{values.map_key}', #{values.other_number})"
        ),
        query_case(
          :sample,
          "SELECT #{values.column} FROM #{values.table} SAMPLE 1/10 OFFSET 0.5"
        ),
        query_case(
          :settings,
          "SELECT #{values.column} FROM #{values.table} SETTINGS max_threads = #{values.limit}"
        )
      ]
  end

  defp postgres_cases(values) do
    common_cases(values) ++
      [
        query_case(
          :quoted_identifier,
          "SELECT \"#{values.column}\" FROM \"#{values.table}\""
        ),
        query_case(
          :array_subscript,
          "SELECT #{values.column}[#{values.index}] FROM #{values.table}",
          [values.column]
        ),
        query_case(
          :array_literal,
          "SELECT (array[#{values.number}, #{values.other_number}])[1]",
          ["array", ")"]
        ),
        query_case(
          :cast_and_json,
          "SELECT #{values.column}::text, #{values.other_column}->>'#{values.map_key}' FROM #{values.table}"
        ),
        query_case(
          :aggregate_filter,
          "SELECT count(*) FILTER (WHERE #{values.column} > #{values.number}) FROM #{values.table}"
        ),
        query_case(:placeholder, "SELECT $1::text AS parameter_value")
      ]
  end

  defp common_cases(values) do
    [
      query_case(
        :expressions,
        "SELECT #{values.number} + #{values.other_number} AS total, CASE WHEN #{values.column} IS NULL THEN '#{values.text}' ELSE '#{values.other_text}' END AS label FROM #{values.table}"
      ),
      query_case(
        :predicates,
        "SELECT #{values.column} FROM #{values.table} WHERE #{values.column} BETWEEN #{values.number} AND #{values.other_number} OR #{values.other_column} IN (1, 2, 3) ORDER BY #{values.column} LIMIT #{values.limit}"
      ),
      query_case(
        :join,
        "SELECT left_table.#{values.column}, right_table.#{values.other_column} FROM #{values.table} AS left_table JOIN #{values.other_table} AS right_table ON left_table.#{values.column} = right_table.#{values.column}"
      ),
      query_case(
        :aggregation,
        "SELECT #{values.column}, count(*) AS total FROM #{values.table} GROUP BY #{values.column} HAVING count(*) > #{values.index}"
      ),
      query_case(
        :cte_union,
        "WITH generated_rows AS (SELECT #{values.column} FROM #{values.table}) SELECT #{values.column} FROM generated_rows UNION ALL SELECT #{values.other_column} FROM #{values.other_table}"
      )
    ]
  end

  defp query_values do
    fixed_map(%{
      column: mixed_case_identifier("column"),
      dataset: mixed_case_identifier("dataset"),
      index: integer(0..10),
      limit: integer(1..100),
      map_key: map(map_key(), &escape_string/1),
      number: integer(-1_000..1_000),
      other_column: mixed_case_identifier("other_column"),
      other_number: integer(-1_000..1_000),
      other_table: mixed_case_identifier("other_table"),
      other_text: map(sql_string(), &escape_string/1),
      project: mixed_case_identifier("project"),
      table: mixed_case_identifier("table"),
      text: map(sql_string(), &escape_string/1)
    })
  end

  defp mixed_case_identifier(prefix) do
    map(string(:alphanumeric, min_length: 1, max_length: 8), fn suffix ->
      prefix <> "_Mixed_" <> suffix
    end)
  end

  defp sql_string do
    one_of([
      string(:alphanumeric, max_length: 12),
      member_of(["hello world", "metadata.source", "O'Reilly"])
    ])
  end

  defp map_key do
    one_of([
      mixed_case_identifier("key"),
      member_of(["metadata.source", "nested.value", "customer's.plan"])
    ])
  end

  defp escape_string(value), do: String.replace(value, "'", "''")

  defp query_case(family, sql, adjacent_brackets \\ []) do
    %{family: family, sql: sql, adjacent_brackets: adjacent_brackets}
  end
end
