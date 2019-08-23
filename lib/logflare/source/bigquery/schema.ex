defmodule Logflare.Source.BigQuery.Schema do
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Sources
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Logs

  def start_link(%RLS{} = rls) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_token: rls.source_id,
        bigquery_project_id: rls.bigquery_project_id,
        bigquery_dataset_id: rls.bigquery_dataset_id,
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
        type_map: %{},
        field_count: nil
      },
      name: name(rls.source_id)
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)
    Logger.info("BigQuery.Schema started: #{state.source_token}")

    {:ok, state, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    case BigQuery.get_table(state.source_token) do
      {:ok, table} ->
        schema = SchemaBuilder.deep_sort_by_fields_name(table.schema)
        type_map = Logs.Validators.BigQuerySchemaChange.to_typemap(schema)
        field_count = count_fields(type_map)

        Sources.Cache.put_bq_schema(state.source_token, schema)
        {:noreply, %{state | schema: schema, type_map: type_map, field_count: field_count}}

      {:error, _response} ->
        Logger.info("Table schema manager init error: #{state.source_token}")
        {:noreply, state}
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
    sorted = SchemaBuilder.deep_sort_by_fields_name(schema)
    type_map = Logs.Validators.BigQuerySchemaChange.to_typemap(sorted)
    field_count = count_fields(type_map)

    Sources.Cache.put_bq_schema(state.source_token, sorted)
    {:noreply, %{state | schema: sorted, type_map: type_map, field_count: field_count}}
  end

  defp count_fields(type_map) do
    Iteraptor.to_flatmap(type_map)
    |> Enum.count()
  end

  defp name(source_token) do
    String.to_atom("#{source_token}" <> "-schema")
  end
end
