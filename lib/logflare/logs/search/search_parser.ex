defmodule Logflare.Logs.Search.Parser do
  @arithmetic_operators ~w[> >= < <= =]
  @moduledoc false
  def parse(searchq) do
    try do
      result =
        %{clauses: [], searchq: searchq}
        |> extract_timestamp_filter()
        |> extract_quoted_strings()
        |> extract_fields_filter()
        |> build_message_clauses()
        |> Map.get(:clauses)
        |> Enum.map(&maybe_cast_value/1)

      {:ok, result}
    rescue
      e ->
        {:error, inspect(e)}
    end
  end

  def extract_fields_filter(parsemap) do
    # uses non-capturing groups to match
    # either double-quoted values with default equality operator
    # or values following the specified operator
    regex = ~r/(metadata\.[\w\.]+:(?:(?:[\d\w\.~=><]+)|(?:".+")))/

    fields_strings =
      regex |> Regex.scan(parsemap.searchq, capture: :all_but_first) |> List.flatten()

    searchq = Regex.replace(regex, parsemap.searchq, "")

    clauses =
      for fs <- fields_strings do
        [column, operatorvalue] = String.split(fs, ":")
        op_regex = ~r/<=|>=|<|>|~/
        maybe_op = Regex.run(op_regex, operatorvalue)
        [op_val] = maybe_op || ["="]

        value = operatorvalue |> String.replace(op_regex, "") |> String.replace(~S|"|, "")

        %{
          path: column,
          value: value,
          operator: op_val
        }
      end

    %{
      searchq: searchq,
      clauses: parsemap.clauses ++ clauses
    }
  end

  def extract_timestamp_filter(parsemap) do
    r_iso8601_date = ~S/(?:[0-9]{4})(?:-?)(?:1[0-2]|0[1-9])(?:-?)(?:3[01]|0[1-9]|[12][0-9])/

    r_iso8601_datetime =
      ~S/(?:-?(?:[1-9][0-9]*)?[0-9]{4})-(?:1[0-2]|0[1-9])-(?:3[01]|0[1-9]|[12][0-9])T(?:2[0-3]|[01][0-9]):(?:[0-5][0-9]):(?:[0-5][0-9])(?:\.[0-9]+)?(?:Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?/

    # rdate = ~S|\d\d\d\d\-\d\d\-\d\d|

    # rdatetime =
    #   ~S/(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(.[0-9]+)?(Z)?/

    roperator = ~S/>=|<=|<|>/
    regex = ~r/timestamp:(#{roperator})(#{r_iso8601_datetime}|#{r_iso8601_date})/

    matches = Regex.scan(regex, parsemap.searchq, capture: :all_but_first)

    clauses =
      for [operator, datetime] <- matches do
        datetime =
          if String.length(datetime) === 10 do
            {:ok, date} = Date.from_iso8601(datetime)
            date
          else
            {:ok, datetime, _} = DateTime.from_iso8601(datetime)
            datetime
          end

        %{
          path: "timestamp",
          value: datetime,
          operator: operator
        }
      end

    %{
      searchq: String.replace(parsemap.searchq, regex, ""),
      clauses: parsemap.clauses ++ clauses
    }
  end

  def build_message_clauses(parsemap) do
    clauses =
      parsemap.searchq
      |> String.split()
      |> Enum.map(&build_message_clause/1)

    %{parsemap | clauses: parsemap.clauses ++ clauses}
  end

  def build_message_clause(word) do
    %{
      path: "event_message",
      value: word,
      operator: "~"
    }
  end

  def maybe_cast_value(%{operator: op, value: sourcevalue} = c)
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

  def maybe_cast_value(c), do: c

  def extract_quoted_strings(parsemap) do
    searchq = parsemap.searchq

    # uses negative lookbehind to find double quoted strings that are not preceded with :
    regex = ~r/(?<!:)"(.*?)"/

    clauses =
      regex
      |> Regex.scan(searchq, capture: :all_but_first)
      |> List.flatten()
      |> Enum.map(&build_message_clause/1)

    searchq = Regex.replace(regex, searchq, "")

    %{
      searchq: searchq,
      clauses: clauses ++ parsemap.clauses
    }
  end
end
