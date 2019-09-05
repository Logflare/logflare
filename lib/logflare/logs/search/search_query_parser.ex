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
    |> ascii_string([?a..?z, ?A..?Z, ?., ?_, ?0..?9], min: 2)
    |> reduce({List, :to_string, []})
    |> label("metadata field")

  operator =
    choice([
      string(">="),
      string(">"),
      string("<="),
      string("<"),
      string("~"),
      string("..")
    ])

  field_value =
    choice([
      ascii_string([?0..?9], min: 1)
      |> concat(string(".."))
      |> ascii_string([?0..?9], min: 1),
      ascii_string([?a..?z, ?A..?Z, ?0..?9], min: 1)
    ])

  date_or_datetime = ascii_string([?0..?9, ?Z, ?T, ?-, ?:], min: 1)

  timestamp_value =
    choice([
      date_or_datetime |> concat(string("..")) |> concat(date_or_datetime),
      date_or_datetime
    ])

  timestamp_field =
    string("timestamp")
    |> ignore(ascii_char([?:]))
    |> concat(
      choice([
        operator,
        string("") |> replace("=")
      ])
    )
    |> concat(timestamp_value)
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

  negated_field =
    string("-")
    |> ignore()
    |> concat(choice([timestamp_field, metadata_field_op_val, word, quoted_string]))
    |> tag(:negated_field)

  defparsec(
    :parse_query,
    choice([negated_field, timestamp_field, metadata_field_op_val, quoted_string, word])
    |> ignore(optional(ascii_string([?\s, ?\n], min: 1)))
    |> wrap()
    |> repeat()
  )

  def parse(querystring) do
    try do
      result =
        querystring
        |> String.trim()
        |> parse_query()
        |> convert_to_pathvalops()
        |> List.flatten()
        |> Enum.map(&maybe_cast_value/1)

      {:ok, result}
    rescue
      e in MatchError ->
        %MatchError{term: {filter, {:error, errstring}}} = e
        {:error, "#{String.capitalize(Atom.to_string(filter))} parse error: #{errstring}"}

      _e in FunctionClauseError ->
        {:error, "Invalid query! Please consult search syntax guide."}

      e ->
        {:error, inspect(e)}
    end
  end

  @arithmetic_operators ~w[> >= < <= =]
  def convert_to_pathvalops({:ok, matches, "", %{}, _, _}) do
    for [{type, tokens}] <- matches do
      case type do
        t when t in [:word, :quoted_string, :timestamp_field, :metadata_field] ->
          to_path_val_op(t, tokens)

        :negated_field ->
          [{type, tokens}] = tokens

          type
          |> to_path_val_op(tokens)
          |> Map.update!(:operator, &("!" <> &1))
      end
    end
  end

  defp to_path_val_op(tag, [regex]) when tag in ~w(word quoted_string)a do
    %{
      path: "event_message",
      value: regex,
      operator: "~"
    }
  end

  defp to_path_val_op(:metadata_field, [path, operator, value]) do
    %{
      path: path,
      value: maybe_tagged_to_literal(value),
      operator: operator
    }
  end

  defp to_path_val_op(field, [path, "=", lvalue, "..", rvalue])
       when field in ~w(metadata_field timestamp_field)a do
    to_range_path_val_op([path, "=", lvalue, "..", rvalue])
  end

  defp to_path_val_op(:timestamp_field, [_, operator, datetime]) do
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
  end

  defp to_range_path_val_op([path, "=", lvalue, "..", rvalue]) do
    [
      %{
        path: path,
        value: lvalue,
        operator: ">="
      },
      %{
        path: path,
        value: rvalue,
        operator: "<="
      }
    ]
  end

  def maybe_tagged_to_literal({:quoted_string, [literal]}) do
    literal
  end

  def maybe_tagged_to_literal(v), do: v

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
