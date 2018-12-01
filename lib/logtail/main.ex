defmodule Logtail.Main do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  def init(table) do
    logs = :ets.new(table, [:named_table, :set, :protected])
    {:ok, logs}
  end

  def create_table() do

  end
end
