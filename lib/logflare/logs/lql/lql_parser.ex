defmodule Logflare.Lql.Parser do
  @moduledoc false
  import NimbleParsec
  import Logflare.Logs.Search.Parser.Helpers
  alias Logflare.Lql
  alias Logflare.Lql.{FilterRule, ChartRule, Utils}
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
            type = typemap[path]
            maybe_cast_value(rule, type)

          %ChartRule{path: path} = rule ->
            %{rule | value_type: typemap[path]}
        end)
        |> Enum.sort()

      {:ok, rules}
    else
      {:ok, rules, rest, _, {_, _}, _} ->
        Logger.warn("LQL parser: #{inspect(rules)}")
        {:error, "LQL parser doesn't know how to handle this part: #{rest}"}

      {:error, err} ->
        {:error, err}
    end
  catch
    e ->
      {:error, e}
  end

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
