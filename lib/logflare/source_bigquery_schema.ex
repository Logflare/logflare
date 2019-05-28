defmodule Logflare.SourceBigQuerySchema do
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.BigQuery.SourceSchemaBuilder

  def start_link(state) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_token: state[:source_token],
        bigquery_project_id: state[:bigquery_project_id],
        schema: %Model.TableSchema{
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
      name: name(state[:source_token])
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    case BigQuery.get_table(state.source_token, state.bigquery_project_id) do
      {:ok, table} ->
        schema = SourceSchemaBuilder.deep_sort_by_fields_name(table.schema)
        Logger.info("Table schema manager started: #{state.source_token}")
        {:ok, %{state | schema: schema}}

      _ ->
        Logger.info("Table schema manager started: #{state.source_token}")
        {:ok, state}
    end
  end

  def get_state(source_token) do
    GenServer.call(name(source_token), :get)
  end

  def update(source_token, schema) do
    GenServer.cast(name(source_token), {:update, schema})
  end

  def handle_call(:get, _from, state) do
    {:reply, state, state}
  end

  def handle_cast({:update, schema}, state) do
    {:noreply, %{state | schema: SourceSchemaBuilder.deep_sort_by_fields_name(schema)}}
  end

  defp name(source_token) do
    String.to_atom("#{source_token}" <> "-schema")
  end
end
