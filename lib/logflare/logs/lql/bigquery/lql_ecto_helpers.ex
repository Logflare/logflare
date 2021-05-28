defmodule Logflare.Lql.EctoHelpers do
  @moduledoc false
  import Ecto.Query
  alias Logflare.Lql.FilterRule
  @top_level ~w(_PARTITIONDATE _PARTITIONTIME event_message timestamp id)

  @spec apply_filter_rules_to_query(Ecto.Query.t(), [FilterRule.t()], keyword) :: Ecto.Query.t()
  def apply_filter_rules_to_query(query, filter_rules, opts \\ [adapter: :bigquery])

  def apply_filter_rules_to_query(query, [], _opts) do
    query
  end

  def apply_filter_rules_to_query(query, rules, _opts) do
    {top_level_filters, other_filters} = Enum.split_with(rules, &(&1.path in @top_level))

    query =
      top_level_filters
      |> Enum.reduce(
        query,
        fn rule, qacc -> where_match_filter_rule(qacc, rule) end
      )

    other_filters
    |> Enum.reduce(
      query,
      fn
        rule, qacc ->
          qacc
          |> unnest_and_join_nested_columns(:inner, rule.path)
          |> where_match_filter_rule(rule)
      end
    )
  end

  def unnest_and_join_nested_columns(q, join_type, path) do
    path
    |> split_by_dots()
    |> Enum.slice(0..-2)
    |> case do
      [] ->
        q

      columns ->
        columns
        |> Enum.with_index(1)
        |> Enum.reduce(q, fn {column, level}, q ->
          column = String.to_atom(column)

          if level === 1 do
            join(q, join_type, [top], n in fragment("UNNEST(?)", field(top, ^column)))
          else
            join(q, join_type, [..., n1], n in fragment("UNNEST(?)", field(n1, ^column)))
          end
        end)
    end
  end

  defp split_by_dots(str) do
    str
    |> String.split(".")
    |> List.wrap()
  end

  def where_match_filter_rule(q, rule) do
    column =
      rule.path
      |> split_by_dots()
      |> List.last()
      |> String.to_atom()

    if not is_nil(rule.values) and rule.operator == :range do
      [lvalue, rvalue] = rule.values
      where(q, [..., n1], fragment("? BETWEEN ? AND ?", field(n1, ^column), ^lvalue, ^rvalue))
    else
      where(q, ^dynamic_where_filter_rule(column, rule.operator, rule.value, rule.modifiers))
    end
  end

  @type operators :: :< | :<= | := | :> | :>= | :"~" | :list_includes | :string_contains
  @spec dynamic_where_filter_rule(atom(), operators, any, map()) :: Ecto.Query.DynamicExpr.t()
  def dynamic_where_filter_rule(c, op, v, modifiers) do
    clause =
      case op do
        :> ->
          dynamic([..., n1], field(n1, ^c) > ^v)

        :>= ->
          dynamic([..., n1], field(n1, ^c) >= ^v)

        :< ->
          dynamic([..., n1], field(n1, ^c) < ^v)

        :<= ->
          dynamic([..., n1], field(n1, ^c) <= ^v)

        := ->
          case v do
            :NULL -> dynamic([..., n1], fragment(~s|? IS NULL|, field(n1, ^c)))
            _ -> dynamic([..., n1], field(n1, ^c) == ^v)
          end

        :"~" ->
          dynamic([..., n1], fragment(~s|REGEXP_CONTAINS(?, ?)|, field(n1, ^c), ^v))

        :string_contains ->
          dynamic([..., n1], fragment(~s|STRPOS(?, ?) > 0|, field(n1, ^c), ^v))

        :list_includes ->
          dynamic([..., n1], fragment(~s|? IN UNNEST(?)|, ^v, field(n1, ^c)))
      end

    if is_negated?(modifiers) do
      dynamic([..., n1], not (^clause))
    else
      clause
    end
  end

  def is_negated?(modifiers), do: Map.get(modifiers, :negate)
end
