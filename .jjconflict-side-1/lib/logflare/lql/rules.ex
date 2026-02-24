defmodule Logflare.Lql.Rules do
  @moduledoc """
  Interface module for LQL rule operations and orchestration.

  This module provides cross-cutting concerns and coordination between
  different rule types, while individual rule modules handle their
  specific behaviors.
  """

  import Logflare.Utils.Guards

  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Lql.Rules.SelectRule

  @type lql_rule :: ChartRule.t() | FilterRule.t() | FromRule.t() | SelectRule.t()
  @type lql_rules :: [lql_rule()]

  # =============================================================================
  # Rule Type Extraction
  # =============================================================================

  @doc """
  Extracts all `FilterRule` structs from a mixed list of LQL rules.
  """
  @spec get_filter_rules(lql_rules()) :: [FilterRule.t()]
  def get_filter_rules(lql_rules) when is_list(lql_rules) do
    Enum.filter(lql_rules, &match?(%FilterRule{}, &1))
  end

  @doc """
  Extracts all `ChartRule` structs from a mixed list of LQL rules.
  """
  @spec get_chart_rules(lql_rules()) :: [ChartRule.t()]
  def get_chart_rules(lql_rules) when is_list(lql_rules) do
    Enum.filter(lql_rules, &match?(%ChartRule{}, &1))
  end

  @doc """
  Extracts all `SelectRule` structs from a mixed list of LQL rules.
  """
  @spec get_select_rules(lql_rules()) :: [SelectRule.t()]
  def get_select_rules(lql_rules) when is_list(lql_rules) do
    Enum.filter(lql_rules, &match?(%SelectRule{}, &1))
  end

  @doc """
  Finds the first `ChartRule` in the list, or returns nil if none exists.
  """
  @spec get_chart_rule(lql_rules()) :: ChartRule.t() | nil
  def get_chart_rule(lql_rules) when is_list(lql_rules) do
    Enum.find(lql_rules, &match?(%ChartRule{}, &1))
  end

  @doc """
  Finds the `FromRule` in the list, or returns nil if none exists.

  Only one from rule is allowed per query, so this returns the first rule if more than one exists.
  """
  @spec get_from_rule(lql_rules()) :: FromRule.t() | nil
  def get_from_rule(lql_rules) when is_list(lql_rules) do
    Enum.find(lql_rules, &match?(%FromRule{}, &1))
  end

  # =============================================================================
  # Cross-Rule Operations
  # =============================================================================

  @doc """
  Normalizes all rules in the collection by applying rule-specific normalization.
  """
  @spec normalize_all_rules(lql_rules()) :: lql_rules()
  def normalize_all_rules(lql_rules) when is_list(lql_rules) do
    {select_rules, other_rules} =
      Enum.split_with(lql_rules, &match?(%SelectRule{}, &1))

    normalized_select_rules = SelectRule.normalize(select_rules)

    # Future: add normalization for other rule types as needed
    # {filter_rules, remaining_rules} = Enum.split_with(other_rules, &match?(%FilterRule{}, &1))
    # normalized_filter_rules = FilterRule.normalize_timestamps(filter_rules)

    normalized_select_rules ++ other_rules
  end

  @doc """
  Extracts all field paths from select rules in the LQL rule collection.
  """
  @spec get_selected_fields(lql_rules()) :: [String.t()]
  def get_selected_fields(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> get_select_rules()
    |> Enum.map(& &1.path)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  Returns true if any select rule uses wildcard selection (s:*).
  """
  @spec has_wildcard_selection?(lql_rules()) :: boolean()
  def has_wildcard_selection?(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> get_select_rules()
    |> Enum.any?(& &1.wildcard)
  end

  # =============================================================================
  # Chart Rule Helpers
  # =============================================================================

  @doc """
  Extracts the chart period from the first chart rule, or returns the default value.
  """
  @spec get_chart_period(lql_rules(), default :: any()) :: atom()
  def get_chart_period(lql_rules, default \\ nil) when is_list(lql_rules) do
    case get_chart_rule(lql_rules) do
      %ChartRule{} = chart -> ChartRule.get_period(chart)
      nil -> default
    end
  end

  @doc """
  Extracts the chart aggregate from the first chart rule, or returns the default value.
  """
  @spec get_chart_aggregate(lql_rules(), default :: any()) :: atom()
  def get_chart_aggregate(lql_rules, default \\ nil) when is_list(lql_rules) do
    case get_chart_rule(lql_rules) do
      %ChartRule{} = chart -> ChartRule.get_aggregate(chart)
      nil -> default
    end
  end

  @doc """
  Updates the period of the first chart rule in the collection.
  """
  @spec put_chart_period(lql_rules(), atom()) :: lql_rules()
  def put_chart_period(lql_rules, period) when is_list(lql_rules) and is_atom_value(period) do
    case Enum.find_index(lql_rules, &match?(%ChartRule{}, &1)) do
      nil ->
        lql_rules

      index ->
        chart = Enum.at(lql_rules, index)
        updated_chart = ChartRule.update(chart, %{period: period})
        List.replace_at(lql_rules, index, updated_chart)
    end
  end

  @doc """
  Updates an existing `ChartRule` with new parameters, or adds the default if none exists.
  """
  @spec update_chart_rule(lql_rules(), ChartRule.t(), params :: map()) :: lql_rules()
  def update_chart_rule(lql_rules, %ChartRule{} = default, params)
      when is_list(lql_rules) and is_map(params) do
    case Enum.find_index(lql_rules, &match?(%ChartRule{}, &1)) do
      nil ->
        [default | lql_rules]

      index ->
        chart = Enum.at(lql_rules, index)
        updated_chart = ChartRule.update(chart, params)
        List.replace_at(lql_rules, index, updated_chart)
    end
  end

  @doc """
  Adds a new `ChartRule` only if no chart rule already exists.
  """
  @spec put_new_chart_rule(lql_rules(), ChartRule.t()) :: lql_rules()
  def put_new_chart_rule(lql_rules, %ChartRule{} = chart) when is_list(lql_rules) do
    case Enum.any?(lql_rules, &match?(%ChartRule{}, &1)) do
      true -> lql_rules
      false -> [chart | lql_rules]
    end
  end

  # =============================================================================
  # From Rule Helpers
  # =============================================================================

  @doc """
  Removes a `FromRule`, if present.
  """
  @spec remove_from_rule(lql_rules()) :: lql_rules()
  def remove_from_rule(lql_rules) when is_list(lql_rules) do
    Enum.reject(lql_rules, &match?(%FromRule{}, &1))
  end

  # =============================================================================
  # Factory Functions
  # =============================================================================

  @doc """
  Returns a default `ChartRule` with standard settings.
  """
  @spec default_chart_rule() :: ChartRule.t()
  def default_chart_rule do
    ChartRule.build(
      aggregate: :count,
      path: "timestamp",
      period: :minute,
      value_type: :datetime
    )
  end

  @doc """
  Returns a default `SelectRule` with wildcard selection.
  """
  @spec default_select_rule() :: SelectRule.t()
  def default_select_rule do
    SelectRule.build(
      path: "*",
      wildcard: true
    )
  end

  # =============================================================================
  # Filter Rule Helpers
  # =============================================================================

  @doc """
  Extracts timestamp filter rules from the LQL rule collection.

  Returns only `FilterRule` structs where the path is "timestamp".
  """
  @spec get_timestamp_filters(lql_rules()) :: [FilterRule.t()]
  def get_timestamp_filters(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> get_filter_rules()
    |> FilterRule.extract_timestamp_filters()
  end

  @doc """
  Extracts non-timestamp filter rules from the LQL rule collection.

  Returns `FilterRule` structs where the path is NOT "timestamp" (metadata and message filters).
  """
  @spec get_metadata_and_message_filters(lql_rules()) :: [FilterRule.t()]
  def get_metadata_and_message_filters(lql_rules) when is_list(lql_rules) do
    lql_rules
    |> get_filter_rules()
    |> FilterRule.extract_metadata_filters()
  end

  @doc """
  Replaces all `FilterRule` structs that match provided path. Preserves index
  position of the first matching rule.

  Appends when no match exists.
  """
  @spec upsert_filter_rule_by_path(lql_rules(), FilterRule.t()) :: lql_rules()
  def upsert_filter_rule_by_path(lql_rules, %FilterRule{path: path} = filter_rule)
      when is_list(lql_rules) do
    case Enum.find_index(lql_rules, &match?(%FilterRule{path: ^path}, &1)) do
      nil ->
        lql_rules ++ [filter_rule]

      index ->
        lql_rules
        |> Enum.reject(&match?(%FilterRule{path: ^path}, &1))
        |> List.insert_at(index, filter_rule)
    end
  end

  @doc """
  Updates timestamp rules in the LQL rule collection with new timestamp rules.

  Removes all existing timestamp filters and replaces them with the provided new rules.
  """
  @spec update_timestamp_rules(lql_rules(), [FilterRule.t()]) :: lql_rules()
  def update_timestamp_rules(lql_rules, new_timestamp_rules)
      when is_list(lql_rules) and is_list(new_timestamp_rules) do
    lql_rules
    |> Enum.reject(&match?(%FilterRule{path: "timestamp"}, &1))
    |> Enum.concat(new_timestamp_rules)
  end

  @doc """
  Creates new timestamp filters by jumping forward or backward in time.

  Takes existing timestamp filters, calculates the time difference, and creates
  new range filters shifted by that interval in the specified direction.
  """
  @spec jump_timestamp(lql_rules(), :backwards | :forwards) :: lql_rules()
  def jump_timestamp(lql_rules, direction)
      when is_list(lql_rules) and direction in [:backwards, :forwards] do
    timestamp_filters = get_timestamp_filters(lql_rules)
    new_timestamp_rules = FilterRule.jump_timestamps(timestamp_filters, direction)

    update_timestamp_rules(lql_rules, new_timestamp_rules)
  end

  @doc """
  Checks if a `FilterRule` uses timestamp shorthand notation.
  """
  @spec timestamp_filter_rule_is_shorthand?(FilterRule.t()) :: boolean()
  def timestamp_filter_rule_is_shorthand?(%FilterRule{} = filter_rule) do
    FilterRule.shorthand_timestamp?(filter_rule)
  end

  # =============================================================================
  # LQL Parser Warnings
  # =============================================================================

  @doc """
  Returns parser warnings for specific LQL dialects.

  Currently supports `:routing` dialect warnings.
  """
  @spec get_lql_parser_warnings(lql_rules(), Keyword.t()) :: String.t() | nil
  def get_lql_parser_warnings(lql_rules, dialect: :routing) when is_list(lql_rules) do
    case Enum.find(lql_rules, &(&1.path == "timestamp")) do
      nil -> nil
      _ -> "Timestamp LQL clauses are ignored for event routing"
    end
  end
end
