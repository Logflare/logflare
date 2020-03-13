defmodule Logflare.Lql.Utils do
  @moduledoc false
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql.{FilterRule, ChartRule}

  def get_filter_rules(rules) do
    rules
    |> Enum.filter(&match?(%FilterRule{}, &1))
    |> Enum.sort()
  end

  def get_chart_rules(rules) do
    rules
    |> Enum.filter(&match?(%ChartRule{}, &1))
    |> Enum.sort()
  end

  def get_ts_filters(rules) do
    Enum.filter(rules, &(&1.path == "timestamp"))
  end

  def get_meta_and_msg_filters(rules) do
    Enum.filter(rules, &(&1.path != "timestamp"))
  end

  def get_lql_parser_warnings(lql_rules, dialect: :routing) when is_list(lql_rules) do
    cond do
      Enum.find(lql_rules, &(&1.path == "timestamp")) ->
        "Timestamp LQL clauses are ignored for event routing"

      Enum.find(lql_rules, &(&1.path == "timestamp")) ->
        "Timestamp LQL clauses are ignored for event routing"

      true ->
        nil
    end
  end
end
