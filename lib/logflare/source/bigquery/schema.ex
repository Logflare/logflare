defmodule Logflare.Source.BigQuery.Schema do
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Sources

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
        },
        schema_not_sorted: %{}
      },
      name: name(state[:source_token])
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    case BigQuery.get_table(state.source_token, state.bigquery_project_id) do
      {:ok, table} ->
        schema = SchemaBuilder.deep_sort_by_fields_name(table.schema)
        Logger.info("Table schema manager started: #{state.source_token}")
        Sources.Cache.put_bq_schema(state.source_token, schema)
        {:ok, %{state | schema: schema, schema_not_sorted: table.schema}}

      _ ->
        Logger.info("Table schema manager started: #{state.source_token}")
        {:ok, state}
    end
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
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
    {:noreply, %{state | schema: SchemaBuilder.deep_sort_by_fields_name(schema)}}
  end

  defp name(source_token) do
    String.to_atom("#{source_token}" <> "-schema")
  end
end
