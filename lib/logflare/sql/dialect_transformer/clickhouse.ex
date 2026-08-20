defmodule Logflare.Sql.DialectTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1, is_pos_integer: 1]

  alias Logflare.Sql.Parser
  alias Logflare.User

  @impl true
  def quote_style, do: nil

  @impl true
  def dialect, do: "clickhouse"

  @impl true
  def transform_source_name(source_name, _data), do: source_name

  @doc """
  Applies an exact upper bound to the top-level result while preserving any
  existing, stricter limit and offset.
  """
  @spec apply_limit(String.t(), pos_integer()) :: {:ok, String.t()} | {:error, String.t()}
  def apply_limit(query, max_rows)
      when is_non_empty_binary(query) and is_pos_integer(max_rows) do
    with {:ok, statements} <- Parser.parse(dialect(), query) do
      apply_limit_to_statements(statements, query, max_rows)
    end
  end

  defp apply_limit_to_statements(
         [%{"Query" => query_ast} = statement],
         _query,
         max_rows
       ) do
    if requires_outer_limit?(query_ast) do
      wrap_with_limit(statement, max_rows)
    else
      with {:ok, limit} <- build_limit(query_ast["limit"], max_rows) do
        statement
        |> put_in(["Query", "limit"], limit)
        |> Parser.to_string()
      end
    end
  end

  # Endpoint validation admits EXPLAIN in addition to Query statements. Preserve
  # it and let the adaptor's decoded-row cap provide the fallback bound.
  defp apply_limit_to_statements([%{"Explain" => _explain}], query, _max_rows),
    do: {:ok, query}

  defp apply_limit_to_statements(_statements, _query, _max_rows),
    do: {:error, "Expected one ClickHouse query"}

  # Set operations need an outer query so ClickHouse applies and parses the cap
  # against the completed result instead of a UNION/INTERSECT/EXCEPT branch.
  # LIMIT BY is per group, while negative, fractional, and arbitrary expressions
  # do not have ordinary row-count semantics. Cap their completed result from an
  # outer query instead of comparing the original expression numerically.
  defp requires_outer_limit?(%{"body" => %{"SetOperation" => _operation}}), do: true
  defp requires_outer_limit?(%{"limit_by" => [_ | _]}), do: true
  defp requires_outer_limit?(%{"limit" => nil}), do: false

  defp requires_outer_limit?(%{
         "limit" => %{"Value" => %{"Number" => [value, _long]}}
       }) do
    case Integer.parse(value) do
      {_integer, ""} -> false
      _other -> true
    end
  end

  defp requires_outer_limit?(_query_ast), do: true

  defp wrap_with_limit(%{"Query" => query_ast} = statement, max_rows) do
    format_clause = query_ast["format_clause"]
    statement = put_in(statement, ["Query", "format_clause"], nil)

    with {:ok, inner_query} <- Parser.to_string(statement),
         {:ok, [%{"Query" => outer_query} = outer_statement]} <-
           Parser.parse(dialect(), "SELECT * FROM (#{inner_query}) LIMIT #{max_rows}") do
      outer_statement
      |> put_in(["Query", "format_clause"], format_clause || outer_query["format_clause"])
      |> Parser.to_string()
    end
  end

  defp build_limit(nil, max_rows), do: parse_limit("SELECT 1 LIMIT #{max_rows}")

  defp build_limit(existing_limit, max_rows) do
    with {:ok, limit} <- parse_limit("SELECT 1 LIMIT least(1, #{max_rows})") do
      limited =
        update_in(limit, ["Function", "args", "List", "args"], fn [_placeholder, maximum] ->
          [%{"Unnamed" => %{"Expr" => existing_limit}}, maximum]
        end)

      {:ok, limited}
    end
  end

  defp parse_limit(query) do
    with {:ok, [%{"Query" => %{"limit" => limit}}]} <- Parser.parse(dialect(), query) do
      {:ok, limit}
    end
  end

  @doc """
  Builds transformation data for ClickHouse from a user and base data.

  Since ClickHouse does not require project/dataset metadata, we can just pass through the base data.
  """
  @spec build_transformation_data(User.t(), map()) :: map()
  def build_transformation_data(%User{}, base_data), do: base_data
end
