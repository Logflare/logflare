defmodule Logflare.Sources.BufferCounters do
  @moduledoc """
  Maintains a count of log events inside the Source.BigQuery.Pipeline Broadway pipeline.
  """

  use GenServer

  require Logger

  @ets_table_name :buffer_counters

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(state) do
    Logger.info("BufferCounters table started!")

    :ets.new(@ets_table_name, [
      :ordered_set,
      :public,
      :named_table,
      decentralized_counters: true,
      read_concurrency: true,
      write_concurrency: true
    ])

    {:ok, state}
  end
end
