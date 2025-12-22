defmodule Logflare.Logs.GroupedSourceRouting do
  @moduledoc false

  alias Logflare.Rules
  alias Logflare.Rules.Rule

  def prepare(source) do
    rules = Rules.Cache.list_rules(source)
    rules_by_id = rules |> Map.new(fn rule -> {rule.id, rule} end)
    RuleSet.make(rules)
  end

  def matching_rules(le, rule_set) do
    for {op, nested_ops} <- rule_set, reduce: [] do
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

  defp match_rule(le_value, {:equal, expected}, {:route, rule_ids}, acc) do
    if le_value == expected do
      List.wrap(rule_ids) ++ acc
    else
      acc
    end
  end
end

defmodule RuleSet do
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Rules.Rule

  # Groups all the rules associated with source by path in a tree
  def make(rules) do
    for rule <- rules, filter <- rule.lql_filters do
      target = rule.id

      reverse_path = String.split(filter.path, ".") |> Enum.reverse()

      reverse_path
      |> Enum.reduce(%{to_command(filter) => {:route, target}}, fn k, acc ->
        %{{:get, k} => acc}
      end)
    end
    |> Enum.reduce(&deep_merge/2)
  end

  defp to_command(%FilterRule{operator: :range, values: [l_val, r_val]}) do
    # TODO: negate
    {:range, {l_val, r_val}}
  end

  defp to_command(%FilterRule{operator: :"~", value: value}) do
    # TODO: negate
    {:match, value}
  end

  defp to_command(%FilterRule{operator: :=, value: value}) do
    # TODO: negate
    {:equal, value}
  end

  defp to_command(%FilterRule{operator: :string_contains, value: value}) do
    # TODO: negate
    {:string_contains, value}
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
