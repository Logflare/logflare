defmodule Logflare.Sql.DialectTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1, is_pos_integer: 1]

  alias Logflare.Sql.Parser
  alias Logflare.User

  @set_operation_result_modifiers ~w(order_by limit offset limit_by)
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
         [%{"Query" => _query_ast} = statement],
         _query,
         max_rows
       ) do
    with {:ok, statement} <- maybe_wrap_set_operation(statement) do
      apply_limit_to_query(statement, max_rows)
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

  defp maybe_wrap_set_operation(%{"Query" => query_ast} = statement) do
    case set_operation_query(query_ast) do
      nil -> {:ok, statement}
      query_ast -> statement |> put_in(["Query"], query_ast) |> wrap_set_operation()
    end
  end

  defp set_operation_query(%{"body" => %{"SetOperation" => _operation}} = query_ast),
    do: query_ast

  defp set_operation_query(%{"body" => %{"Query" => nested_query}} = query_ast) do
    case set_operation_query(nested_query) do
      nil ->
        nil

      nested_query ->
        if redundant_query_wrapper?(query_ast), do: nested_query, else: query_ast
    end
  end

  defp set_operation_query(_query_ast), do: nil

  defp redundant_query_wrapper?(query_ast) do
    query_ast
    |> Map.delete("body")
    |> Enum.all?(fn {_key, value} -> is_nil(value) or value == [] end)
  end

  # ClickHouse binds trailing modifiers to a set-operation branch. Move modifiers
  # onto a SELECT over the completed result when its ordering only references
  # exposed output columns. Otherwise preserve the coupled clauses for backwards
  # compatibility and apply only the endpoint cap outside them.
  defp wrap_set_operation(%{"Query" => query_ast} = statement) do
    modifier_keys = set_operation_modifier_keys(query_ast)
    modifiers = Map.take(query_ast, modifier_keys)

    inner_query_ast = Enum.reduce(modifier_keys, query_ast, &clear_query_modifier(&2, &1))
    statement = put_in(statement, ["Query"], inner_query_ast)

    with {:ok, inner_query} <- Parser.to_string(statement),
         {:ok, [%{"Query" => outer_query_ast} = outer_statement]} <-
           Parser.parse(dialect(), "SELECT * FROM (#{inner_query})") do
      {:ok, put_in(outer_statement, ["Query"], Map.merge(outer_query_ast, modifiers))}
    end
  end

  defp set_operation_modifier_keys(query_ast) do
    if safe_to_hoist_result_modifiers?(query_ast) do
      @set_operation_result_modifiers ++ @terminal_modifiers
    else
      @terminal_modifiers
    end
  end

  defp safe_to_hoist_result_modifiers?(query_ast) do
    safe_to_hoist_order_by?(query_ast) and
      safe_to_hoist_limit_by?(query_ast) and
      not contains_nested_query?(query_ast["limit"]) and
      not contains_nested_query?(query_ast["offset"])
  end

  defp safe_to_hoist_order_by?(%{"order_by" => nil}), do: true

  defp safe_to_hoist_order_by?(
         %{
           "order_by" => %{"exprs" => order_exprs, "interpolate" => nil}
         } = query_ast
       ) do
    case set_operation_output_names(query_ast["body"]) do
      {:ok, output_names} ->
        Enum.all?(order_exprs, &exposed_order_identifier?(&1, output_names))

      :error ->
        false
    end
  end

  defp safe_to_hoist_order_by?(_query_ast), do: false

  defp safe_to_hoist_limit_by?(%{"limit_by" => []}), do: true

  defp safe_to_hoist_limit_by?(%{"limit_by" => limit_by} = query_ast)
       when is_list(limit_by) do
    case set_operation_output_names(query_ast["body"]) do
      {:ok, output_names} ->
        Enum.all?(limit_by, &exposed_result_expression?(&1, output_names))

      :error ->
        false
    end
  end

  defp safe_to_hoist_limit_by?(_query_ast), do: false

  defp exposed_result_expression?(%{"Identifier" => identifier}, output_names) do
    MapSet.member?(output_names, identifier_key(identifier))
  end

  # Qualified names lose their table scope above the set result, while nested
  # queries can refer to CTEs or branch-local tables that are no longer visible.
  defp exposed_result_expression?(%{"CompoundIdentifier" => _identifiers}, _output_names),
    do: false

  defp exposed_result_expression?(%{"Query" => _query}, _output_names), do: false
  defp exposed_result_expression?(%{"Subquery" => _query}, _output_names), do: false
  defp exposed_result_expression?(%{"Exists" => _query}, _output_names), do: false
  defp exposed_result_expression?(%{"InSubquery" => _query}, _output_names), do: false

  defp exposed_result_expression?(expression, output_names) when is_list(expression) do
    Enum.all?(expression, &exposed_result_expression?(&1, output_names))
  end

  defp exposed_result_expression?(expression, output_names) when is_map(expression) do
    Enum.all?(expression, fn {_key, value} ->
      exposed_result_expression?(value, output_names)
    end)
  end

  defp exposed_result_expression?(_expression, _output_names), do: true

  defp contains_nested_query?(%{"Query" => _query}), do: true
  defp contains_nested_query?(%{"Subquery" => _query}), do: true
  defp contains_nested_query?(%{"Exists" => _query}), do: true
  defp contains_nested_query?(%{"InSubquery" => _query}), do: true

  defp contains_nested_query?(value) when is_list(value),
    do: Enum.any?(value, &contains_nested_query?/1)

  defp contains_nested_query?(value) when is_map(value),
    do: Enum.any?(value, fn {_key, child} -> contains_nested_query?(child) end)

  defp contains_nested_query?(_value), do: false

  defp set_operation_output_names(%{"Query" => query_ast}),
    do: set_operation_output_names(query_ast["body"])

  defp set_operation_output_names(%{"SetOperation" => %{"left" => left}}),
    do: set_operation_output_names(left)

  defp set_operation_output_names(%{"Select" => %{"projection" => projection}}) do
    # A wildcard's expanded names depend on the source schema and cannot be
    # proven from the parser AST. Keep only explicit output names.
    output_names =
      Enum.reduce(projection, MapSet.new(), fn
        %{"ExprWithAlias" => %{"alias" => identifier}}, output_names ->
          MapSet.put(output_names, identifier_key(identifier))

        %{"UnnamedExpr" => %{"Identifier" => identifier}}, output_names ->
          MapSet.put(output_names, identifier_key(identifier))

        _projection, output_names ->
          output_names
      end)

    {:ok, output_names}
  end

  defp set_operation_output_names(_body), do: :error

  defp exposed_order_identifier?(%{"expr" => %{"Identifier" => identifier}}, output_names) do
    MapSet.member?(output_names, identifier_key(identifier))
  end

  defp exposed_order_identifier?(_order_expr, _output_names), do: false

  defp identifier_key(%{"quote_style" => quote_style, "value" => value}),
    do: {quote_style, value}

  defp clear_query_modifier(query_ast, "limit_by"), do: Map.put(query_ast, "limit_by", [])
  defp clear_query_modifier(query_ast, key), do: Map.put(query_ast, key, nil)

  # LIMIT BY is per group, while negative, fractional, and arbitrary expressions
  # do not have ordinary row-count semantics. Cap their completed result from an
  # outer query instead of comparing the original expression numerically.
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
