defmodule Logflare.Sources.SourceRouter.RulesTree do
  @moduledoc false
  alias Logflare.LogEvent
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.Lql.Rules.FilterRule

  import Logflare.Utils, only: [stringify: 1]

  @behaviour Logflare.Sources.SourceRouter

  @type t() :: [entry()]
  @type entry() ::
          {key(), [entry()]}
          | {operator(), route()}
          | {:eq_index, eq_index()}
  @type route() :: {:route, target() | [target()]}
  @type target() :: Rule.id() | {Rule.id(), filter_set()}
  @type key() :: binary()
  @type operator() :: {atom(), any()}
  @type eq_index() :: %{term() => target() | [target()]}

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

  # Hash-indexed positive equality: replaces a linear scan of sibling `:=` leaves
  # at this path with a single lookup. Negated equality stays as a regular leaf.
  defp find_matches(le_value, :eq_index, index_map, acc) when is_map(index_map) do
    case Map.get(index_map, le_value) do
      nil -> acc
      rule_ids -> accumulate(rule_ids, acc)
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
        expected in List.wrap(le_value)

      :list_includes_regexp ->
        le_value
        |> List.wrap()
        |> Enum.any?(&(stringify(&1) =~ expected))

      :string_contains ->
        String.contains?(stringify(le_value), stringify(expected))

      := ->
        le_value == expected

      :"~" ->
        stringify(le_value) =~ expected

      op when op in [:<=, :<, :>=, :>] ->
        apply(Kernel, operator, [le_value, expected])
    end
  end

  defp accumulate_if(true, rule_ids, acc), do: accumulate(rule_ids, acc)
  defp accumulate_if(false, _rule_ids, acc), do: acc

  # Accumulate: handle list of rule ids
  defp accumulate([], acc), do: acc
  defp accumulate([h | tail], acc), do: accumulate(tail, accumulate(h, acc))

  # Accumulate: single-filter rule without bitmask, use bare Rule.id()
  defp accumulate(rule_id, acc) when is_integer(rule_id),
    do: Map.put(acc, rule_id, 0)

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
  - `{:eq_index, %{value => target | [target]}}` - hash-indexed fold of sibling positive `:=` leaves at a path node; one `Map.get/2` replaces a linear scan over equal-shape rules
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
        |- {:eq_index, %{8 => rule(0)}}
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
        {"field1", [{:eq_index, %{8 => 0}}]},
        {"field2", [{{:~, "sth"}, {:route, 1}}]}
      ]}
  ]
  ```
  Single-filter rule targets are stored as bare `Rule.id()` integers (instead of
  `{Rule.id(), filter_set()}` tuples).
  Multi-filter rules keep the tuple form so the bitmask accumulates across paths.

  You can find more examples by looking at `rules_tree_test.exs`
  """
  @spec build([Rule.t()]) :: t()
  def build(rules) do
    for rule <- rules,
        filters_num = length(rule.lql_filters),
        {filter, index} <- Enum.with_index(rule.lql_filters) do
      target = build_target(rule.id, filters_num, index)

      reverse_path = String.split(filter.path, ".") |> Enum.reverse()

      # Generates nested map: #{"key1" => %{"key2" => %{operator => route}}}
      reverse_path
      |> Enum.reduce(%{to_operator(filter) => {:route, target}}, fn k, acc ->
        %{k => acc}
      end)
    end
    |> deep_group_keys()
  end

  # Single-filter rule: emit bare rule_id. The accumulate clause for integers
  # puts straight to 0 (matched), bypassing apply_filter_bitmask entirely.
  defp build_target(rule_id, 1, 0), do: rule_id

  defp build_target(rule_id, filters_num, index),
    do: {rule_id, build_filter_bitmask(filters_num, index)}

  defp to_operator(%FilterRule{modifiers: %{negate: true}} = rule) do
    {:not, to_operator(%{rule | modifiers: %{rule.modifiers | negate: false}})}
  end

  defp to_operator(%FilterRule{operator: :range, values: [l_val, r_val]}) do
    {:range, {l_val, r_val}}
  end

  defp to_operator(%FilterRule{operator: op, value: value})
       when op in [:"~", :list_includes_regex] do
    {op, Regex.compile!(value, "u")}
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
    {eq_entries, rest_entries} =
      Enum.split_with(kv, fn
        {{:=, _value}, {:route, _targets}} -> true
        _ -> false
      end)

    rest = for {k, v} <- rest_entries, do: {k, deep_to_list(v)}

    case eq_entries do
      [] ->
        rest

      _ ->
        index =
          Map.new(eq_entries, fn {{:=, value}, {:route, targets}} -> {value, targets} end)

        [{:eq_index, index} | rest]
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
