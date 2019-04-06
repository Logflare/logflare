defmodule Logflare.TableBigQuerySchema do
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model

  def start_link(website_table) do
    GenServer.start_link(__MODULE__, %{source: website_table, schema: %Model.TableSchema{}},
      name: name(website_table)
    )
  end

  def init(state) do
    {:ok, table} = BigQuery.get_table(state.source)
    schema = table.schema
    Logger.info("Table schema manager started: #{state.source}")
    {:ok, %{state | schema: schema}}
  end

  def get(website_table) do
    GenServer.call(name(website_table), :get)
  end

  def update(website_table, schema) do
    GenServer.cast(name(website_table), {:update, schema})
  end

  def handle_call(:get, _from, state) do
    {:reply, state.schema, state}
  end

  def handle_cast({:update, schema}, state) do
    {:noreply, %{state | schema: schema}}
  end

  defp name(website_table) do
    String.to_atom("#{website_table}" <> "-schema")
  end
end
