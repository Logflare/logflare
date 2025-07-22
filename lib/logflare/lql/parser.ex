defmodule Logflare.Lql.Parser do
  @moduledoc """
  Core LQL parsing logic which relies on `NimbleParsec`.

  The parsing combinators are defined in `Logflare.Lql.Parser.Combinators`
  and supporting functions are in `Logflare.Lql.Parser.Helpers`.
  """

  import NimbleParsec
  import Logflare.Lql.Parser.Combinators
  import Logflare.Lql.Parser.Helpers

  require Logger

  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule

  defparsec(
    :do_parse,
    concat(
      optional(string("-") |> replace(:negate)),
      choice([
        chart_clause(),
        select_clause(),
        timestamp_clause(),
        metadata_level_clause(),
        metadata_clause(),
        field_clause(),
        quoted_string(:quoted_event_message),
        word()
      ])
    )
    |> optional(ignore(choice([ascii_string([?\s, ?\n], min: 1), eos()])))
    |> reduce(:maybe_apply_negation_modifier)
    |> times(min: 1, max: 100)
  )

  @doc """
  `parse/1` allows for parsing of an LQL statement without validating against a provided BQ schema.
  This allows for parse-only workflows, as coupling validations with the parsing makes things more complex.
  """
  @spec parse(String.t()) :: {:ok, Rules.lql_rules()}
  def parse(nil), do: {:ok, []}
  def parse(""), do: {:ok, []}

  def parse(querystring) do
    {:ok, rules, _, _, _, _} =
      querystring
      |> String.trim()
      |> do_parse()

    {chart_rule_tokens, remaining_rules} =
      rules
      |> List.flatten()
      |> Enum.split_with(fn
        {:chart, _} -> true
        _ -> false
      end)

    {select_rule_tokens, other_rules} =
      remaining_rules
      |> Enum.split_with(fn
        {:select, _} -> true
        _ -> false
      end)

    chart_rule =
      if not Enum.empty?(chart_rule_tokens) do
        chart_rule =
          chart_rule_tokens
          |> Enum.reduce(%{}, fn {:chart, fields}, acc -> Map.merge(acc, Map.new(fields)) end)

        ChartRule.build(Map.to_list(chart_rule))
      end

    select_rules =
      select_rule_tokens
      |> Enum.map(fn {:select, fields} ->
        select_rule =
          fields
          |> Map.new()
          |> Map.put(:wildcard, Map.get(Map.new(fields), :path) == "*")

        struct!(SelectRule, select_rule)
      end)

    filter_rules =
      Enum.map(other_rules, fn rule ->
        maybe_cast_value(rule)
      end)

    rules =
      ([chart_rule | select_rules] ++ filter_rules)
      |> List.flatten()
      |> Enum.reject(&is_nil/1)

    {:ok, rules}
  end

  def parse("", _schema) do
    {:ok, []}
  end

  def parse(querystring, %TS{} = schema) do
    parsed =
      querystring
      |> String.trim()
      |> do_parse()

    case parsed do
      {:ok, rules, "", _, {_, _}, _} ->
        {chart_rule_tokens, remaining_rules} =
          rules
          |> List.flatten()
          |> Enum.split_with(fn
            {:chart, _} -> true
            _ -> false
          end)

        {select_rule_tokens, other_rules} =
          remaining_rules
          |> Enum.split_with(fn
            {:select, _} -> true
            _ -> false
          end)

        typemap = SchemaUtils.bq_schema_to_flat_typemap(schema)

        chart_rule =
          if not Enum.empty?(chart_rule_tokens) do
            chart_rule =
              chart_rule_tokens
              |> Enum.reduce(%{}, fn {:chart, fields}, acc -> Map.merge(acc, Map.new(fields)) end)
              |> then(&Map.put(&1, :value_type, get_path_type(typemap, &1.path, querystring)))

            ChartRule.build(Map.to_list(chart_rule))
          end

        select_rules =
          select_rule_tokens
          |> Enum.map(fn {:select, fields} ->
            select_rule =
              fields
              |> Map.new()
              |> Map.put(:wildcard, Map.get(Map.new(fields), :path) == "*")

            struct!(SelectRule, select_rule)
          end)

        rules =
          Enum.map(other_rules, fn
            %FilterRule{path: path} = rule ->
              type = get_path_type(typemap, path, querystring)
              maybe_cast_value(rule, type)
          end)

        rules =
          ([chart_rule | select_rules] ++ rules)
          |> List.flatten()
          |> Enum.reject(&is_nil/1)

        {:ok, rules}

      {:ok, _rules, rest, _, {_, _}, _} ->
        {:error, "LQL parser doesn't know how to handle this part: #{rest}"}

      {:error, err} ->
        {:error, err}

      {:error, err, _, _, _, _} ->
        {:error, err}
    end
  catch
    {suggested_querystring, err} ->
      {:error, :field_not_found, suggested_querystring, err}

    err ->
      {:error, err}
  end

  defp get_path_type(typemap, path, _querystring) do
    type = Map.get(typemap, path)

    case type do
      :map ->
        throw(
          {"",
           [
             "Field type `#{type}` is not queryable.",
             "",
             ""
           ]}
        )

      nil ->
        throw(
          {"",
           [
             "LQL parser error: path `#{path}` not present in source schema.",
             "",
             ""
           ]}
        )

      _type ->
        type
    end
  end

  # cast without typing, best effort
  defp maybe_cast_value(%{value: "true"} = c), do: %{c | value: true}
  defp maybe_cast_value(%{value: "false"} = c), do: %{c | value: false}

  defp maybe_cast_value(%{value: v} = c) do
    parsed =
      case Integer.parse(v) do
        {num, ""} ->
          num

        _ ->
          case Float.parse(v) do
            {num, ""} -> num
            _ -> v
          end
      end

    %{c | value: parsed}
  end

  defp maybe_cast_value(c, {:list, type}), do: maybe_cast_value(c, type)

  defp maybe_cast_value(%{values: values, value: nil} = c, type) when values != [] do
    %{
      c
      | values:
          values
          |> Enum.map(fn data ->
            %{value: data, path: c.path}
            |> maybe_cast_value(type)
            |> Map.fetch!(:value)
          end)
    }
  end

  defp maybe_cast_value(%{value: :NULL} = c, _), do: c
  defp maybe_cast_value(%{value: "true"} = c, :boolean), do: %{c | value: true}
  defp maybe_cast_value(%{value: "false"} = c, :boolean), do: %{c | value: false}

  defp maybe_cast_value(%{value: v, path: p}, :boolean),
    do: throw("Query syntax error: Expected boolean for #{p}, got: '#{v}'")

  defp maybe_cast_value(%{value: v, path: p} = c, type)
       when is_binary(v) and type in [:integer, :float] do
    mod =
      case type do
        :integer -> Integer
        :float -> Float
      end

    value =
      case mod.parse(v) do
        {value, ""} -> value
        _ -> throw("Query syntax error: expected #{type} for #{p}, got: '#{v}'")
      end

    %{c | value: value}
  end

  defp maybe_cast_value(c, :string), do: c
  defp maybe_cast_value(c, :datetime), do: c
  # questionable if this can be reached
  defp maybe_cast_value(c, :naive_datetime), do: c

  # also questionable if this can be reached
  defp maybe_cast_value(c, nil) do
    throw("Query parsing error: attempting to cast value #{c.value} to nil type for #{c.path}")
  end
end
