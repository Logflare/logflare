defmodule Logflare.Sources.RateCounters do
  @moduledoc false

  use GenServer

  require Logger

  @ets_table_name :rate_counters

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(state) do
    :ets.new(@ets_table_name, [:public, :named_table])
    {:ok, state}
  end
end
