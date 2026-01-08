defmodule Logflare.Sources.SourceRouter.GroupedRuleset do
  @moduledoc false
  alias Logflare.Rules

  @behaviour Logflare.Sources.SourceRouter

  @impl true
  def matching_rules(event, source) do
    rule_set = Rules.Cache.ruleset_by_source_id(source.id)

    matching_rule_ids(event, rule_set)
    |> Enum.flat_map(fn
      {id, 0} -> [id]
      {_id, _matches_left} -> []
    end)
    |> Rules.Cache.get_rules()
  end

  def matching_rule_ids(le, rule_set) do
    for {op, nested_ops} <- rule_set, reduce: %{} do
      acc -> match_rule(le.body, op, nested_ops, acc)
    end
  end

  defp match_rule(event_part, {:get, key}, _ops, acc)
       when not is_map(event_part) or not is_map_key(event_part, key) do
    acc
  end

  defp match_rule(event_part, {:get, key}, ops, acc)
       when is_map_key(event_part, key) and is_map(ops) do
    sub_part = Map.get(event_part, key)

    for {op, nested_ops} <- ops, reduce: acc do
      acc -> match_rule(sub_part, op, nested_ops, acc)
    end
  end

  # Handle operators
  defp match_rule(le_value, {:not, {operator, expected}}, {:route, rule_ids}, acc) do
    check_op(operator, le_value, expected)
    |> Kernel.not()
    |> match_cond(rule_ids, acc)
  end

  defp match_rule(le_value, {operator, expected}, {:route, rule_ids}, acc) do
    check_op(operator, le_value, expected)
    |> match_cond(rule_ids, acc)
  end

  defp match_rule(_le_value, {_op, _value}, {:route, _rule_ids}, acc), do: acc

  defp check_op(operator, le_value, expected) do
    case operator do
      nil ->
        false

      :range ->
        {lvalue, rvalue} = expected
        le_value >= lvalue and le_value <= rvalue

      :list_includes ->
        le_value == expected

      :list_includes_regexp ->
        stringify(le_value) =~ ~r/#{expected}/u

      :string_contains ->
        String.contains?(stringify(le_value), stringify(expected))

      := ->
        le_value == expected

      :"~" ->
        stringify(le_value) =~ ~r/#{expected}/u

      op when op in [:<=, :<, :>=, :>] ->
        apply(Kernel, operator, [le_value, expected])
    end
  end

  defp match_cond(true, rule_ids, acc), do: accumulate(rule_ids, acc)
  defp match_cond(false, _rule_ids, acc), do: acc

  defp accumulate([], acc), do: acc
  defp accumulate([h | tail], acc), do: accumulate(tail, accumulate(h, acc))

  defp accumulate({rule_id, _filters_num}, acc) when is_map_key(acc, rule_id),
    do: %{acc | rule_id => acc[rule_id] - 1}

  defp accumulate({rule_id, filters_num}, acc), do: Map.put(acc, rule_id, filters_num - 1)

  defp stringify(v) when is_integer(v) do
    Integer.to_string(v)
  end

  defp stringify(v) when is_float(v) do
    Float.to_string(v)
  end

  defp stringify(v) when is_binary(v), do: v
  defp stringify(v), do: inspect(v)

  alias Logflare.Lql.Rules.FilterRule

  # Groups all the rules associated with source by path in a tree
  def make_ruleset(rules) do
    for rule <- rules, filters_num = length(rule.lql_filters), filter <- rule.lql_filters do
      target = {rule.id, filters_num}

      reverse_path = String.split(filter.path, ".") |> Enum.reverse()

      reverse_path
      |> Enum.reduce(%{to_command(filter) => {:route, target}}, fn k, acc ->
        %{{:get, k} => acc}
      end)
    end
    |> Enum.reduce(&deep_merge/2)
  end

  defp to_command(%FilterRule{modifiers: %{negate: true}} = rule) do
    {:not, to_command(%{rule | negate: false})}
  end

  defp to_command(%FilterRule{operator: :range, values: [l_val, r_val]}) do
    {:range, {l_val, r_val}}
  end

  defp to_command(%FilterRule{operator: op, value: value}) do
    {op, value}
  end

  defp deep_merge(a, b) do
    Map.merge(a, b, &merger/3)
  end

  defp merger(_k, val_a, val_b) when is_map(val_a) and is_map(val_b) do
    Map.merge(val_a, val_b, &merger/3)
  end

  defp merger(_k, {:route, a}, {:route, b}) do
    {:route, List.wrap(a) ++ List.wrap(b)}
  end

  defp merger(_k, val_a, val_b) when is_tuple(val_a) and is_tuple(val_b) do
    [val_a, val_b]
  end
end
