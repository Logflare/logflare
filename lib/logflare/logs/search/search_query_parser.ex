defmodule Logflare.Logs.Search.Parser do
  @moduledoc false
  import NimbleParsec
  alias Logflare.Logs.Validators.BigQuerySchemaChange

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

  chart_option =
    string("chart")
    |> ignore(ascii_char([?:]))
    |> concat(metadata_field)
    |> tag(:chart_option)

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
      ascii_string([?0..?9, ?.], min: 1)
      |> concat(string(".."))
      |> ascii_string([?0..?9, ?.], min: 1),
      ascii_string([?a..?z, ?A..?Z, ?0..?9, ?.], min: 1)
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
    choice([
      chart_option,
      negated_field,
      timestamp_field,
      metadata_field_op_val,
      quoted_string,
      word
    ])
    |> ignore(optional(ascii_string([?\s, ?\n], min: 1)))
    |> wrap()
    |> repeat()
  )

  def parse(querystring, schema) do
    typemap =
      schema
      |> BigQuerySchemaChange.to_typemap()
      |> Iteraptor.to_flatmap()
      |> Enum.map(fn {k, v} -> {String.trim_trailing(k, ".t"), v} end)
      |> Enum.map(fn {k, v} -> {String.replace(k, ".fields.", "."), v} end)
      |> Enum.uniq()
      |> Enum.reject(fn {_k, v} -> v === :map end)
      |> Map.new()

    {:ok, path_val_ops} = do_parse(querystring)

    result =
      for %{path: path} = path_val_op <- path_val_ops do
        maybe_cast_value(path_val_op, typemap[path])
      end

    %{search: search, chart: chart} = group_by_type(result)

    chart =
      if chart do
        %{chart | value: typemap[chart.path]}
      else
        chart
      end

    {:ok, %{search: search, chart: chart}}
  catch
    e ->
      {:error, e}
  rescue
    e in MatchError ->
      %MatchError{term: {filter, {:error, errstring}}} = e
      {:error, "#{String.capitalize(Atom.to_string(filter))} parse error: #{errstring}"}

    e in FunctionClauseError ->
      {:error, "Invalid query! Please consult search syntax guide."}
  end

  def do_parse(querystring) do
    result =
      querystring
      |> String.trim()
      |> parse_query()
      |> convert_to_pathvalops()
      |> List.flatten()

    {:ok, result}
  end

  def group_by_type(pathvalops) do
    chart = Enum.find(pathvalops, &(&1.operator == "chart"))

    %{
      chart: chart,
      search: Enum.sort(Enum.reject(pathvalops, &(&1.operator == "chart")))
    }
  end

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

        :chart_option ->
          [_, metadata_field] = tokens
          %{operator: "chart", path: metadata_field, value: nil}
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

  defp to_path_val_op(:timestamp_field, [_, operator, datetime]) do
    dt =
      if String.length(datetime) === 10 do
        case Date.from_iso8601(datetime) do
          {:ok, date} ->
            date

          {:error, err} ->
            throw(
              "Query syntax error: timestamp expected date or datetime string in ISO8601 format, got: #{
                datetime
              }, error: #{err}"
            )
        end
      else
        case DateTime.from_iso8601(datetime) do
          {:ok, dt, _offset} ->
            dt

          {:error, err} ->
            throw(
              "Query syntax error: timestamp expected date or datetime string in ISO8601 format, got: #{
                datetime
              }, error: #{err}"
            )
        end
      end

    %{
      path: "timestamp",
      value: dt,
      operator: operator
    }
  end

  def maybe_tagged_to_literal({:quoted_string, [literal]}) do
    literal
  end

  def maybe_tagged_to_literal(v), do: v

  defp not_quote(<<?", _::binary>>, context, _, _), do: {:halt, context}
  defp not_quote(_, context, _, _), do: {:cont, context}

  defp maybe_cast_value(%{value: "true"} = c, :boolean), do: %{c | value: true}
  defp maybe_cast_value(%{value: "false"} = c, :boolean), do: %{c | value: false}

  defp maybe_cast_value(c, :boolean),
    do: throw("Query syntax error: Expected boolean for #{c.path}, got: #{c.value}")

  defp maybe_cast_value(%{value: sourcevalue} = c, :integer) when is_binary(sourcevalue) do
    value =
      case Integer.parse(sourcevalue) do
        {value, ""} -> value
        _ -> throw("Query syntax error: expected integer for #{c.path}, got: #{sourcevalue}")
      end

    %{c | value: value}
  end

  defp maybe_cast_value(%{value: sourcevalue} = c, :float) when is_binary(sourcevalue) do
    value =
      case Float.parse(sourcevalue) do
        {value, ""} -> value
        _ -> throw("Query syntax error: expected float for #{c.path}, got: #{sourcevalue}")
      end

    %{c | value: value}
  end

  # Handles chart pathvalop casting
  defp maybe_cast_value(%{value: nil} = c, :integer), do: c

  defp maybe_cast_value(c, :string), do: c

  defp maybe_cast_value(c, :datetime), do: c

  defp maybe_cast_value(c, :naive_datetime), do: c

  defp maybe_cast_value(c, nil) do
    throw("Query parsing error: attempting to cast value #{c.value} to nil type for #{c.path}")
  end
end
