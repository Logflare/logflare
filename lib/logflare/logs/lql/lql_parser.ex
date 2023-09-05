defmodule Logflare.Lql.Parser do
  @moduledoc false
  import NimbleParsec
  import __MODULE__.Helpers

  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule

  require Logger

  defparsec(
    :do_parse,
    concat(
      optional(string("-") |> replace(:negate)),
      choice([
        chart_clause(),
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

  def parse("", _schema) do
    {:ok, []}
  end

  def parse(querystring, %TS{} = schema) do
    with {:ok, rules, "", _, {_, _}, _} <-
           querystring
           |> String.trim()
           |> do_parse() do
      {chart_rule_tokens, other_rules} =
        rules
        |> List.flatten()
        |> Enum.split_with(fn
          {:chart, _} -> true
          _ -> false
        end)

      typemap = SchemaUtils.bq_schema_to_flat_typemap(schema)

      chart_rule =
        if not Enum.empty?(chart_rule_tokens) do
          chart_rule =
            chart_rule_tokens
            |> Enum.reduce(%{}, fn {:chart, fields}, acc -> Map.merge(acc, Map.new(fields)) end)
            |> then(&Map.put(&1, :value_type, get_path_type(typemap, &1.path, querystring)))

          struct!(ChartRule, chart_rule)
        end

      rules =
        Enum.map(other_rules, fn
          %FilterRule{path: path} = rule ->
            type = get_path_type(typemap, path, querystring)
            maybe_cast_value(rule, type)
        end)

      rules =
        [chart_rule | rules]
        |> List.flatten()
        |> Enum.reject(&is_nil/1)

      {:ok, rules}
    else
      {:ok, rules, rest, _, {_, _}, _} ->
        dbg(rules)
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

  defp maybe_cast_value(c, {:list, type}), do: maybe_cast_value(c, type)

  defp maybe_cast_value(%{values: values, value: nil} = c, type) when length(values) >= 1 do
    %{
      c
      | values:
          values
          |> Enum.map(&%{value: &1, path: c.path})
          |> Enum.map(&maybe_cast_value(&1, type))
          |> Enum.map(& &1.value)
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
  defp maybe_cast_value(c, :naive_datetime), do: c

  defp maybe_cast_value(c, nil) do
    throw("Query parsing error: attempting to cast value #{c.value} to nil type for #{c.path}")
  end
end
