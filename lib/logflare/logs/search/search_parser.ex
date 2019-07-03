defmodule Logflare.Logs.Search.Parser do
  @moduledoc false
  def parse(searchq) do
    result = build_message_clauses(searchq)
    {:ok, result}
  end

  def build_message_clauses(searchq) do
    for word <- String.split(searchq) do
      %{
        path: "metadata.message",
        value: word,
        operator: "~"
      }
    end
  end
end
