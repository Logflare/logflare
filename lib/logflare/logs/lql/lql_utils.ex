defmodule Logflare.Lql.Utils do
  @moduledoc false
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql.{FilterRule, ChartRule}
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS

  @spec bq_schema_to_flat_typemap(TS.t()) :: map
  def bq_schema_to_flat_typemap(%TS{} = schema) do
    schema
    |> SchemaUtils.to_typemap()
    |> Iteraptor.to_flatmap()
    |> Enum.map(fn {k, v} -> {String.trim_trailing(k, ".t"), v} end)
    |> Enum.map(fn {k, v} -> {String.replace(k, ".fields.", "."), v} end)
    |> Enum.uniq()
    |> Enum.reject(fn {_k, v} -> v === :map end)
    |> Map.new()
  end


  def build_message_filter_rule_from_regex(regex) when is_binary(regex) do
    %FilterRule{
      operator: "~",
      path: "event_message",
      value: regex,
      modifiers: []
    }
  end

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

  def get_lql_parser_warnings(lql_rules, dialect: :routing) when is_list(lql_rules) do
    cond do
      Enum.find(lql_rules, &(&1.path == "timestamp")) ->
        "Timestamp LQL clauses are ignored for event routing"

      true ->
        nil
    end
  end
end
