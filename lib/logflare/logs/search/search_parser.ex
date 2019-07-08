defmodule Logflare.Logs.Search.Parser do
  @arithmetic_operators ~w[> >= < <= =]
  @moduledoc false
  def parse(searchq) do
    result =
      %{clauses: [], searchq: searchq}
      |> extract_timestamp_filter()
      |> extract_quoted_strings()
      |> extract_fields_filter()
      |> build_message_clauses()
      |> Map.get(:clauses)
      |> Enum.map(&maybe_cast_value/1)

    {:ok, result}
  end

  def extract_fields_filter(parsemap) do
    # uses non-capturing groups to match
    # either double-quoted values with default equality operator
    # or values following the specified operator
    regex = ~r/(metadata\.[\w\.]+:(?:(?:[\d\w\.~=><]+)|(?:".+")))/

    fields_strings =
      Regex.scan(regex, parsemap.searchq, capture: :all_but_first) |> List.flatten()

    searchq = Regex.replace(regex, parsemap.searchq, "")

    clauses =
      for fs <- fields_strings do
        [column, operatorvalue] = String.split(fs, ":")
        op_regex = ~r/<=|>=|<|>|~/
        maybe_op = Regex.run(op_regex, operatorvalue)
        [op_val] = maybe_op || ["="]

        %{
          path: column,
          value: String.replace(operatorvalue, op_regex, "") |> String.replace(~S|"|, ""),
          operator: op_val
        }
      end

    %{
      searchq: searchq,
      clauses: parsemap.clauses ++ clauses
    }
  end

  def extract_timestamp_filter(parsemap) do
    rdate = ~S|\d\d\d\d\-\d\d\-\d\d|
    rdatetime = ~S|[\d\-TZ\:\+]+|

    roperator = ~S/>=|<=|<|>/
    regex = ~r/timestamp:(#{roperator})(#{rdatetime}|#{rdate})/

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
    clauses = for word <- String.split(parsemap.searchq), do: build_message_clause(word)
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

    quoted_strings = Regex.scan(regex, searchq, capture: :all_but_first) |> List.flatten()

    searchq = Regex.replace(regex, searchq, "")

    clauses = for(qs <- quoted_strings, do: build_message_clause(qs))

    %{
      searchq: searchq,
      clauses: clauses ++ parsemap.clauses
    }
  end
end
