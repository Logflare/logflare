defmodule Logflare.Sql.DialectTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1, is_pos_integer: 1]

  alias Logflare.Sql.Parser
  alias Logflare.User

  @terminal_modifiers ~w(format_clause settings)

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
    case set_operation_query(query_ast) do
      nil ->
        apply_limit_to_query(statement, max_rows)

      query_ast ->
        statement
        |> put_in(["Query"], query_ast)
        |> wrap_with_limit(max_rows)
    end
  end

  # Endpoint validation admits EXPLAIN in addition to Query statements. Preserve
  # it and let the adaptor's decoded-row cap provide the fallback bound.
  defp apply_limit_to_statements([%{"Explain" => _explain}], query, _max_rows),
    do: {:ok, query}

  defp apply_limit_to_statements(_statements, _query, _max_rows),
    do: {:error, "Expected one ClickHouse query"}

  defp apply_limit_to_query(%{"Query" => query_ast} = statement, max_rows) do
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

  # ClickHouse binds trailing result modifiers to the final branch of an
  # unparenthesized set operation. Preserve those modifiers in place and add the
  # endpoint cap around the completed result. Only SETTINGS and FORMAT move to
  # the generated outer query because they must remain terminal.
  defp set_operation_query(%{"body" => %{"SetOperation" => _operation}} = query_ast),
    do: query_ast

  defp set_operation_query(%{"body" => %{"Query" => nested_query}} = query_ast) do
    case set_operation_query(nested_query) do
      nil ->
        nil

      nested_query ->
        if collapsible_query_wrapper?(query_ast) do
          move_terminal_modifiers(query_ast, nested_query)
        else
          query_ast
        end
    end
  end

  defp set_operation_query(_query_ast), do: nil

  defp collapsible_query_wrapper?(query_ast) do
    query_ast
    |> Map.drop(["body" | @terminal_modifiers])
    |> Enum.all?(fn {_key, value} -> is_nil(value) or value == [] end)
  end

  defp move_terminal_modifiers(query_ast, nested_query) do
    Enum.reduce(@terminal_modifiers, nested_query, fn key, nested_query ->
      value = merge_terminal_modifier(key, nested_query[key], query_ast[key])

      if is_nil(value) or value == [],
        do: nested_query,
        else: Map.put(nested_query, key, value)
    end)
  end

  defp merge_terminal_modifier("settings", nested, outer)
       when is_list(nested) and is_list(outer),
       do: nested ++ outer

  defp merge_terminal_modifier(_key, nested, outer) when is_nil(outer) or outer == [],
    do: nested

  defp merge_terminal_modifier(_key, _nested, outer), do: outer

  # LIMIT BY, FETCH, negative, fractional, and arbitrary expressions do not have
  # ordinary row-count semantics. Cap their completed result from an outer query
  # instead of comparing the original expression numerically.
  defp requires_outer_limit?(%{"fetch" => fetch}) when not is_nil(fetch), do: true
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
    terminal_modifiers = Map.take(query_ast, @terminal_modifiers)

    statement =
      Enum.reduce(@terminal_modifiers, statement, fn key, statement ->
        put_in(statement, ["Query", key], nil)
      end)

    with {:ok, inner_query} <- Parser.to_string(statement),
         {:ok, [%{"Query" => outer_query} = outer_statement]} <-
           Parser.parse(dialect(), "SELECT * FROM (#{inner_query}) LIMIT #{max_rows}") do
      outer_statement
      |> put_in(["Query"], Map.merge(outer_query, terminal_modifiers))
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
