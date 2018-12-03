defmodule Logtail.Main do
  use GenServer

  @table :unused_table

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  def init(table) do
    :ets.new(@table, [:named_table, :ordered_set, :public])
    |> IO.inspect(label: "ETS Table Created")
    {:ok, table}
  end

  def create_table_or_insert_log(table_name, timestamp, log_entry) do
    case :ets.info(table_name) do
      :undefined ->
        :ets.new(table_name, [:named_table, :ordered_set, :public])
        :ets.insert(table_name, {timestamp, log_entry})
      _ ->
        :ets.insert(table_name, {timestamp, log_entry})
    end
  end

end
