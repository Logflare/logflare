defmodule Logtail.Main do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  def init(logs_table) do
    logs_table = :ets.new(:logs_table, [:named_table, :set, :protected])
    |> IO.inspect(label: "ETS Table Created")
    {:ok, logs_table}
  end

end
