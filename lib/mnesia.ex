defmodule Logflare.Mnesia do
  def clear_table(tab) when is_atom(tab) do
    :mnesia.clear_table(tab)
  end
end
