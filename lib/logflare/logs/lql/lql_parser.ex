defmodule Logflare.Lql.Parser do
  @moduledoc false
  import NimbleParsec
  import __MODULE__.Helpers
  alias Logflare.Lql
  alias Logflare.Lql.{FilterRule, ChartRule, Utils}
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
        quoted_string(:event_message),
        word()
      ])
    )
    |> ignore(choice([ascii_string([?\s, ?\n], min: 1), eos()]))
    |> reduce(:maybe_apply_negation_modifier)
    |> times(min: 1, max: 100)
  )

  def parse("", _schema) do
    {:ok, [%FilterRule{path: "event_message", operator: "~", value: ".+", modifiers: []}]}
  end

  def parse(querystring, schema) do
    with {:ok, rules, "", _, {_, _}, _} <-
           querystring
           |> String.trim()
           |> do_parse() do
      typemap = Lql.Utils.bq_schema_to_flat_typemap(schema)

      rules =
        rules
        |> List.flatten()
        |> Enum.map(fn
          %FilterRule{path: path} = rule ->
            type = get_path_type(typemap, path)
            maybe_cast_value(rule, type)

          %ChartRule{path: path} = rule ->
            type = get_path_type(typemap, path)
            %{rule | value_type: type}
        end)
        |> Enum.sort()

      {:ok, rules}
    else
      {:ok, rules, rest, _, {_, _}, _} ->
        Logger.warn("LQL parser: #{inspect(rules)}")
        {:error, "LQL parser doesn't know how to handle this part: #{rest}"}

      {:error, err} ->
        {:error, err}

      {:error, err, _, _, _, _} ->
        {:error, err}
    end
  catch
    e ->
      {:error, e}
  end

  defp get_path_type(typemap, path) do
    type = Map.get(typemap, path)

    if type do
      type
    else
      maybe_this = get_most_similar_path(typemap, path)

      throw(
        "LQL Parser error: path '#{path}' not present in source schema. Did you mean '#{
          maybe_this
        }'?"
      )
    end
  end

  defp get_most_similar_path(paths, user_path) do
    paths
    |> Enum.map(fn {k, _} -> k end)
    |> Enum.max_by(&String.jaro_distance(&1, user_path))
  end

  defp maybe_cast_value(%{value: :NULL} = c, _), do: c
  defp maybe_cast_value(%{value: "true"} = c, :boolean), do: %{c | value: true}
  defp maybe_cast_value(%{value: "false"} = c, :boolean), do: %{c | value: false}

  defp maybe_cast_value(%{value: v, path: p}, :boolean),
    do: throw("Query syntax error: Expected boolean for #{p}, got: #{v}")

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
        _ -> throw("Query syntax error: expected #{type} for #{p}, got: #{v}")
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
