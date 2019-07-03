defmodule Logflare.Logs.Search.Parser do
  @moduledoc false
  def parse(searchq) do
    result =
      searchq
      |> extract_quoted_strings()
      |> build_message_clauses()
      |> Map.get(:clauses)

    {:ok, result}
  end

  def build_message_clauses(parsemap) do
    clauses = for word <- String.split(parsemap.searchq), do: build_message_clause(word)
    %{parsemap | clauses: parsemap.clauses ++ clauses}
  end

  def build_message_clause(word) do
    %{
      path: "metadata.message",
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
