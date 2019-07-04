defmodule Logflare.Logs.Search.Parser do
  @moduledoc false
  def parse(searchq) do
    result =
      searchq
      |> extract_quoted_strings()
      |> extract_fields_filter()
      |> build_message_clauses()
      |> Map.get(:clauses)

    {:ok, result}
  end

  def extract_fields_filter(parsemap) do
    regex = ~r/(metadata\.[\w\.]+:[\d\w\.~=><]+)/
    fields_strings = Regex.scan(regex, parsemap.searchq, capture: :all_but_first)
    searchq = Regex.replace(regex, parsemap.searchq, "")

    clauses =
      for [fs] <- fields_strings do
        [column, operatorvalue] = String.split(fs, ":")
        op_regex = ~r/<=|>=|<|>|~/
        maybe_op = Regex.run(op_regex, operatorvalue)
        [op_val] = maybe_op || ["="]

        %{
          path: column,
          value: String.replace(operatorvalue, op_regex, ""),
          operator: op_val
        }
      end

    %{
      searchq: searchq,
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

  def extract_quoted_strings(searchq) do
    regex = ~r/"(.+)"/
    quoted_strings = Regex.run(regex, searchq, capture: :all_but_first)
    searchq = Regex.replace(regex, searchq, "")

    clauses =
      if quoted_strings do
        for(qs <- quoted_strings, do: build_message_clause(qs))
      else
        []
      end

    %{
      searchq: searchq,
      clauses: clauses
    }
  end
end
