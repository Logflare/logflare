defmodule Logflare.Logs do
  alias Logflare.Table

  @spec insert_or_push(:atom, {{}, %{}}) :: true
  def insert_or_push(source_token, event) do
    if :ets.info(source_token) == :undefined do
      Table.push(source_token, event)
      true
    else
      :ets.insert(source_token, event)
    end
  end
end
