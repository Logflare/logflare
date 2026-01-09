defmodule Logflare.Sources.SourceRouter.RulesTree do
  @moduledoc false
  alias Logflare.Rules
  alias Logflare.Lql.Rules.FilterRule

  import Logflare.Utils, only: [stringify: 1]

  @behaviour Logflare.Sources.SourceRouter

  @impl true
  def matching_rules(event, source) do
    rule_set = Rules.Cache.rules_tree_by_source_id(source.id)

    matching_rule_ids(event, rule_set)
    |> Enum.flat_map(fn
      {id, matches_left} when matches_left <= 0 -> [id]
      {_id, _matches_left} -> []
    end)
    |> Rules.Cache.get_rules()
  end

  def matching_rule_ids(le, rule_set) do
    for {op, nested_ops} <- rule_set, reduce: %{} do
      acc -> match_rule(le.body, op, nested_ops, acc)
    end
  end

  # Handle maps nested in lists
  defp match_rule([], <<_key::binary>>, _ops, acc), do: acc

  defp match_rule([event_part | tail], <<key::binary>>, ops, acc) do
    acc = match_rule(event_part, key, ops, acc)
    match_rule(tail, key, ops, acc)
  end

  # No such key
  defp match_rule(event_part, <<key::binary>>, _ops, acc)
       when not is_map(event_part) or not is_map_key(event_part, key) do
    acc
  end

  defp match_rule(event_part, <<key::binary>>, ops, acc)
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

  defp check_op(operator, le_value, expected) do
    case operator do
      nil ->
        false

      :range ->
        {lvalue, rvalue} = expected
        le_value >= lvalue and le_value <= rvalue

      :list_includes ->
        expected in le_value

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

  defp accumulate({rule_id, bitmask}, acc) when is_map_key(acc, rule_id),
    do: %{acc | rule_id => apply_filter_bitmask(acc[rule_id], bitmask)}

  defp accumulate({rule_id, bitmask}, acc),
    do: Map.put(acc, rule_id, bitmask)

  # Groups all the rules associated with source by path in a tree
  def build(rules) do
    for rule <- rules,
        filters_num = length(rule.lql_filters),
        {filter, index} <- Enum.with_index(rule.lql_filters) do
      target = {rule.id, build_filter_bitmask(filters_num, index)}

      reverse_path = String.split(filter.path, ".") |> Enum.reverse()

      reverse_path
      |> Enum.reduce(%{to_command(filter) => {:route, target}}, fn k, acc ->
        %{k => acc}
      end)
    end
    |> Enum.reduce(&deep_merge/2)
  end

  defp to_command(%FilterRule{modifiers: %{negate: true}} = rule) do
    {:not, to_command(%{rule | modifiers: %{rule.modifiers | negate: false}})}
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

  @doc """
  Builds a bitwise registry of matched filters.

  Starting from the least significant (right-most) bit,
  n-th bit corresponds to the state of n-th filter, indexed from 0.

  Bit unset (0) means the filter matched. Thanks to such representation,
  a flagset equal to 0 always means all filters matched
  - regardless of the total number of filters
  """
  def build_filter_flagset(filters_num) when filters_num >= 0 do
    Bitwise.bsl(1, filters_num) - 1
  end

  @doc """
  Builds a bitwise mask for a filter with provided 0-based index

  If applied with bitwise and to the registry, unsets the bit corresponding
  to that filter.

  It also happens to be the same as the representation of registry with only
  one bit unset, thus it can be used as registry value after the first filter match
  """
  def build_filter_bitmask(filters_num, index) do
    base = build_filter_flagset(filters_num)
    Bitwise.bxor(base, Bitwise.bsl(1, index))
  end

  @doc """
  Applies a bitwise mask to filter registry, marking the filter as matching.

  It is idempotent, may be applied to the registry multiple times, yielding the same result.
  """
  def apply_filter_bitmask(flagset, bitmask) do
    Bitwise.band(flagset, bitmask)
  end
end
