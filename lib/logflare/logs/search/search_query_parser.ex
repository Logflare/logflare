defmodule Logflare.Logs.Search.Parser do
  @moduledoc false
  import NimbleParsec

  word =
    ascii_string([48..125], min: 1)
    |> tag(:word)

  quoted_string =
    ignore(ascii_char([?"]))
    |> repeat_while(
      utf8_char([]),
      {:not_quote, []}
    )
    |> ignore(ascii_char([?"]))
    |> reduce({List, :to_string, []})
    |> tag(:quoted_string)

  metadata_field =
    string("metadata")
    |> ascii_string([?a..?z, ?., ?_, ?0..?9], min: 2)
    |> reduce({List, :to_string, []})

  operator =
    choice([
      string(">="),
      string(">"),
      string("<="),
      string("<"),
      string("~")
    ])

  field_value = ascii_string([?a..?z, ?A..?Z, ?0..?9], min: 1)

  timestamp_field =
    string("timestamp")
    |> ignore(ascii_char([?:]))
    |> concat(
      choice([
        operator,
        string("") |> replace("=")
      ])
    )
    |> concat(ascii_string([?0..?9, ?Z, ?T, ?-, ?:], min: 1))
    |> tag(:timestamp_field)

  metadata_field_op_val =
    metadata_field
    |> ignore(ascii_char([?:]))
    |> concat(
      choice([
        operator,
        string("") |> replace("=")
      ])
    )
    |> concat(choice([field_value, quoted_string]))
    |> tag(:metadata_field)

  defparsec :parse_query,
            choice([timestamp_field, metadata_field_op_val, quoted_string, word])
            |> ignore(optional(ascii_string([?\s, ?\n], min: 1)))
            |> wrap()
            |> repeat()

  def parse(querystring) do
    try do
      result =
        querystring
        |> String.trim()
        |> parse_query()
        |> convert_to_pathvalops()
        |> Enum.map(&maybe_cast_value/1)

      {:ok, result}
    rescue
      e in MatchError ->
        %MatchError{term: {filter, {:error, errstring}}} = e
        {:error, "#{String.capitalize(Atom.to_string(filter))} parse error: #{errstring}"}

      e ->
        {:error, inspect(e)}
    end
  end

  @arithmetic_operators ~w[> >= < <= =]
  def convert_to_pathvalops({:ok, matches, "", %{}, _, _}) do
    for [{type, tokens}] <- matches do
      case type do
        t when t in [:word, :quoted_string] ->
          [regex] = tokens

          %{
            path: "event_message",
            value: regex,
            operator: "~"
          }

        :timestamp_field ->
          [_, operator, datetime] = tokens

          datetime =
            if String.length(datetime) === 10 do
              {:timestamp, {:ok, date}} = {:timestamp, Date.from_iso8601(datetime)}
              date
            else
              {:timestamp, {:ok, datetime, _}} = {:timestamp, DateTime.from_iso8601(datetime)}
              datetime
            end

          %{
            path: "timestamp",
            value: datetime,
            operator: operator
          }

        :metadata_field ->
          [path, operator, value] =
            case tokens do
              [path, operator, {:quoted_string, [value]}] -> [path, operator, value]
              ts -> ts
            end

          %{
            path: path,
            value: value,
            operator: operator
          }
      end
    end
  end

  defp not_quote(<<?", _::binary>>, context, _, _), do: {:halt, context}
  defp not_quote(_, context, _, _), do: {:cont, context}

  defp maybe_cast_value(%{value: "true"} = c), do: %{c | value: true}
  defp maybe_cast_value(%{value: "false"} = c), do: %{c | value: false}

  defp maybe_cast_value(%{operator: op, value: sourcevalue} = c)
       when op in @arithmetic_operators and is_binary(sourcevalue) do
    value =
      with :error <- Integer.parse(c.value),
           :error <- Float.parse(c.value) do
        c.value
      else
        {value, ""} -> value
        {_, _} -> c.value
      end

    %{c | value: value}
  end

  defp maybe_cast_value(c), do: c
end
