defmodule Logflare.TableBigQuerySchema do
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model

  def start_link(website_table) do
    GenServer.start_link(
      __MODULE__,
      %{
        source: website_table,
        schema:
          schema = %Model.TableSchema{
            fields: [
              %Model.TableFieldSchema{
                description: nil,
                fields: nil,
                mode: "REQUIRED",
                name: "timestamp",
                type: "TIMESTAMP"
              },
              %Model.TableFieldSchema{
                description: nil,
                fields: nil,
                mode: "NULLABLE",
                name: "event_message",
                type: "STRING"
              }
            ]
          }
      },
      name: name(website_table)
    )
  end

  def init(state) do
    case BigQuery.get_table(state.source) do
      {:ok, table} ->
        schema = table.schema
        Logger.info("Table schema manager started: #{state.source}")
        {:ok, %{state | schema: schema}}

      _ ->
        Logger.info("Table schema manager started: #{state.source}")
        {:ok, state}
    end
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
