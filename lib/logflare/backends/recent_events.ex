defmodule Logflare.Backends.RecentEvents do
  @moduledoc false
  use GenServer

  @ets_table_name :recent_events

  ## Server
  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__, hibernate_after: 1_000)
  end

  def init(_args) do
    :ets.new(@ets_table_name, [:public, :named_table, :duplicate_bag])
    {:ok, %{}}
  end
end
