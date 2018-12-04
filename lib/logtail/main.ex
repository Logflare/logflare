defmodule Logtail.Main do
  use GenServer

  def start_link do
    GenServer.start_link(__MODULE__, %{})
  end

  def init(args) do
    IO.inspect(label: "Genserver Started")
    {:ok, args}
  end

  def handle_cast(table_name, timestamp, log_entry) do
    insert_log(table_name, timestamp, log_entry)
    {:noreply, table_name, timestamp, log_entry}
  end

  def insert_log(table_name, timestamp, log_entry) do
    case :ets.info(table_name) do
      :undefined ->
        table_name
        |> :ets.new([:named_table, :ordered_set, :public])
        |> :ets.insert({timestamp, log_entry})
      _ ->
        :ets.insert(table_name, {timestamp, log_entry})
    end
  end

end
