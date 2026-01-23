defmodule Logflare.Sources.SourceRouter.RulesTree do
  @moduledoc false
  alias Logflare.LogEvent
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.Lql.Rules.FilterRule

  import Logflare.Utils, only: [stringify: 1]

  @behaviour Logflare.Sources.SourceRouter

  @type t() :: [entry()]
  @type entry() :: {key(), [entry()]} | {operator(), [route()]}
  @type route() :: {:route, target() | [target()]}
  @type target() :: {Rule.id(), filter_set()}
  @type key() :: binary()
  @type operator() :: {atom(), any()}

  @type filter_set() :: non_neg_integer()

  @impl true
  def matching_rules(event, source) do
    rule_set = Rules.Cache.rules_tree_by_source_id(source.id)

    matching_rule_ids(event, rule_set)
    |> Rules.Cache.get_rules()
  end

  @doc """
  Finds ids of matching rules.

  The algorithm iterates over rules tree, traversing the log event along with tree nodes.

  For a rule to match, all the filters must match, so the implementation
  accumulates rule ids as keys in a map. As a value, an integer serving as bitwise flag registry
  is stored (see `build_filter_flagset/1`).
  """
  @spec matching_rule_ids(LogEvent.t(), t()) :: [Rule.id()]
  def matching_rule_ids(le, rules_tree) do
    for {op, nested_ops} <- rules_tree, reduce: %{} do
      acc -> find_matches(le.body, op, nested_ops, acc)
    end
    |> Enum.flat_map(fn
      {id, matches_left} when matches_left <= 0 -> [id]
      {_id, _matches_left} -> []
    end)
  end

  # Find matches: find key in log event
  ## Handle maps nested in lists in log event
  defp find_matches([], <<_key::binary>>, _ops, acc), do: acc

  defp find_matches([event_part | tail], <<key::binary>>, ops, acc) do
    # Find matches inside head entry
    acc = find_matches(event_part, key, ops, acc)
    # Iterate over log event on the same level
    find_matches(tail, key, ops, acc)
  end

  ## No such key in log event, break traversal
  defp find_matches(event_part, <<key::binary>>, _ops, acc)
       when not is_map(event_part) or not is_map_key(event_part, key) do
    acc
  end

  ## Key present, go one level deeper in log event and rules tree
  defp find_matches(event_part, <<key::binary>>, ops, acc)
       when is_map_key(event_part, key) and is_list(ops) do
    sub_part = Map.get(event_part, key)

    for {op, nested_ops} <- ops, reduce: acc do
      acc -> find_matches(sub_part, op, nested_ops, acc)
    end
  end

  # Handle operators. Only route should be left in ops.
  defp find_matches(le_value, {:not, {operator, expected}}, {:route, rule_ids}, acc) do
    check_op(operator, le_value, expected)
    |> Kernel.not()
    |> accumulate_if(rule_ids, acc)
  end

  defp find_matches(le_value, {operator, expected}, {:route, rule_ids}, acc) do
    check_op(operator, le_value, expected)
    |> accumulate_if(rule_ids, acc)
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

  defp accumulate_if(true, rule_ids, acc), do: accumulate(rule_ids, acc)
  defp accumulate_if(false, _rule_ids, acc), do: acc

  # Accumulate: handle list of rule ids
  defp accumulate([], acc), do: acc
  defp accumulate([h | tail], acc), do: accumulate(tail, accumulate(h, acc))

  # Accumulate: handle rule id already present in accumulator
  defp accumulate({rule_id, bitmask}, acc) when is_map_key(acc, rule_id),
    do: %{acc | rule_id => apply_filter_bitmask(acc[rule_id], bitmask)}

  # Accumulate: handle rule id not present in accumulator
  defp accumulate({rule_id, bitmask}, acc),
    do: Map.put(acc, rule_id, bitmask)

  @doc """
  Creates a data structure grouping all the rules and their filters associated with a source by a path in a tree.

  You can think of it as of decision tree built with operators - each node is an action taken on ingested log event.
  The operators are:
  - a binary key - a traversal of log event structure by getting that key from a map
  - `{operator, compared_value}` - an operator testing log event value, as in #{inspect(FilterRule)}, always followed by route(s)
  - `{:not, {operator, compared_value}` - negated operator (prevents the need to store modifiers from #{inspect(FilterRule)})
  - `{:route, target}` - always under an operator, indicates where the rule_id that should be used for routing if the operator succeeds

  Such structure allows to access each key inside the log event structure at most once.

  For performance reasons, the whole structure is kept using lists instead of maps.

  ### Example
  For a source with two rules and one filter rule for each:
  `"m.field1:8"` (for rule id `0`) and `"m.field2:~sth"` (rule id `1`)
  The tree conceptually looks like this:

  ```
  |
  |- "metadata"
     |- "field1"
        |- {:=, 8}
           |- {route, rule(0)}
     |- "field2"
        |- {:~, "sth}
           |- {route, rule(1)}
  ```
  where `rule(id)` is a tuple of rule ID and integer filter bitflag
  (see `build_filter_flagset/1` for details).

  The representation in Elixir terms is:

  ```
  [
    {"metadata",
      [
        {"field1", [{{:=, 8}, {:route, {0, 0b0}}}]},
        {"field2", [{{:~, "sth"}, {:route, {1, 0b0}}}]}
      ]}
  ]
  ```

  You can find more examples by looking at `rules_tree_test.exs`
  """
  @spec build([Rule.t()]) :: t()
  def build(rules) do
    for rule <- rules,
        filters_num = length(rule.lql_filters),
        {filter, index} <- Enum.with_index(rule.lql_filters) do
      target = {rule.id, build_filter_bitmask(filters_num, index)}

      reverse_path = String.split(filter.path, ".") |> Enum.reverse()

      # Generates nested map: #{"key1" => %{"key2" => %{operator => route}}}
      reverse_path
      |> Enum.reduce(%{to_operator(filter) => {:route, target}}, fn k, acc ->
        %{k => acc}
      end)
    end
    |> deep_group_keys()
  end

  defp to_operator(%FilterRule{modifiers: %{negate: true}} = rule) do
    {:not, to_operator(%{rule | modifiers: %{rule.modifiers | negate: false}})}
  end

  defp to_operator(%FilterRule{operator: :range, values: [l_val, r_val]}) do
    {:range, {l_val, r_val}}
  end

  defp to_operator(%FilterRule{operator: op, value: value}) do
    {op, value}
  end

  defp deep_group_keys([]), do: []
  defp deep_group_keys(entries), do: Enum.reduce(entries, &deep_merge/2) |> deep_to_list()

  # Merges maps at all levels
  defp deep_merge(a, b) do
    Map.merge(a, b, &merger/3)
  end

  defp merger(_k, val_a, val_b) when is_map(val_a) and is_map(val_b) do
    Map.merge(val_a, val_b, &merger/3)
  end

  defp merger(_k, {:route, a}, {:route, b}) do
    {:route, List.wrap(a) ++ List.wrap(b)}
  end

  defp deep_to_list(kv) when is_map(kv) do
    for {k, v} <- kv do
      {k, deep_to_list(v)}
    end
  end

  defp deep_to_list(val), do: val

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
