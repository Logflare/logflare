defmodule Logflare.Source.BigQuery.Schema do
  @moduledoc """
  Manages the source schema across a cluster.

  Schemas should only be updated once per minute. Server is booted with schema from Postgres. Handles schema mismatch between BigQuery and Logflare.
  """
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources
  alias Logflare.LogEvent

  @persist_every 60_000
  @timeout 60_000

  def start_link(%RLS{} = rls) do
    GenServer.start_link(
      __MODULE__,
      %{
        source_token: rls.source_id,
        bigquery_project_id: rls.bigquery_project_id,
        bigquery_dataset_id: rls.bigquery_dataset_id,
        schema: SchemaBuilder.initial_table_schema(),
        type_map: %{
          event_message: %{t: :string},
          timestamp: %{t: :datetime},
          id: %{t: :string}
        },
        field_count: 3,
        next_update: System.system_time(:second)
      },
      name: name(rls.source_id)
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    persist()

    {:ok, state, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    source = Sources.get_by(token: state.source_token)

    case Sources.get_source_schema_by(source_id: source.id) do
      nil ->
        Sources.Cache.put_bq_schema(state.source_token, state.schema)

        Logger.info("Schema manager init error: #{state.source_token}")

        {:noreply, %{state | next_update: next_update()}}

      source_schema ->
        schema = BigQuery.SchemaUtils.deep_sort_by_fields_name(source_schema.bigquery_schema)
        type_map = BigQuery.SchemaUtils.to_typemap(schema)
        field_count = count_fields(type_map)

        Sources.Cache.put_bq_schema(state.source_token, schema)

        {:noreply,
         %{
           state
           | schema: schema,
             type_map: type_map,
             field_count: field_count,
             next_update: next_update()
         }}
    end
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{
      source_id: state.source_token
    })

    reason
  end

  def get_state(source_token) do
    GenServer.call(name(source_token), :get)
  end

  def update(source_token, log_event) do
    GenServer.call(name(source_token), {:update, log_event}, @timeout)
  end

  def update_cluster(source_token, schema, type_map, field_count) do
    GenServer.multi_call(
      Node.list(),
      name(source_token),
      {:update, schema, type_map, field_count}
    )
  end

  def set_next_update_cluster(source_token) do
    GenServer.multi_call(Node.list(), name(source_token), :set_next_update)
  end

  def handle_call(:get, _from, state) do
    {:reply, state, state}
  end

  def handle_call(:set_next_update, _from, state) do
    {:reply, :ok, %{state | next_update: next_update()}}
  end

  def handle_call({:update, %LogEvent{body: body, id: event_id}}, _from, state) do
    # set_next_update_cluster(state.source_token)

    schema = SchemaBuilder.build_table_schema(body.metadata, state.schema)

    if not same_schemas?(state.schema, schema) and state.next_update < System.system_time(:second) and
         state.field_count < 500 do
      case BigQuery.patch_table(
             state.source_token,
             schema,
             state.bigquery_dataset_id,
             state.bigquery_project_id
           ) do
        {:ok, _table_info} ->
          type_map = BigQuery.SchemaUtils.to_typemap(schema)
          field_count = count_fields(type_map)

          # update_cluster(state.source_token, schema, type_map, field_count)

          Logger.info("Source schema updated from log_event!",
            source_id: state.source_token,
            log_event_id: event_id
          )

          Sources.Cache.put_bq_schema(state.source_token, schema)

          {:reply, :ok,
           %{
             state
             | schema: schema,
               type_map: type_map,
               field_count: field_count,
               next_update: next_update()
           }}

        {:error, response} ->
          case BigQuery.GenUtils.get_tesla_error_message(response) do
            "Provided Schema does not match Table" <> _tail = _message ->
              case BigQuery.get_table(state.source_token) do
                {:ok, table} ->
                  schema = SchemaBuilder.build_table_schema(body.metadata, table.schema)

                  case BigQuery.patch_table(
                         state.source_token,
                         schema,
                         state.bigquery_dataset_id,
                         state.bigquery_project_id
                       ) do
                    {:ok, _table_info} ->
                      type_map = BigQuery.SchemaUtils.to_typemap(schema)
                      field_count = count_fields(type_map)

                      # update_cluster(state.source_token, schema, type_map, field_count)

                      Logger.info("Source schema updated from BigQuery!",
                        source_id: state.source_token,
                        log_event_id: event_id
                      )

                      Sources.Cache.put_bq_schema(state.source_token, schema)

                      {:reply, :ok,
                       %{
                         state
                         | schema: schema,
                           type_map: type_map,
                           field_count: field_count,
                           next_update: next_update()
                       }}

                    {:error, response} ->
                      Logger.warn("Source schema update error!",
                        tesla_response: BigQuery.GenUtils.get_tesla_error_message(response),
                        source_id: state.source_token,
                        log_event_id: event_id
                      )

                      {:reply, :error, %{state | next_update: next_update()}}
                  end

                {:error, response} ->
                  Logger.warn("Source schema update error!",
                    tesla_response: BigQuery.GenUtils.get_tesla_error_message(response),
                    source_id: state.source_token,
                    log_event_id: event_id
                  )

                  {:reply, :error, %{state | next_update: next_update()}}
              end

            message ->
              Logger.warn("Source schema update error!",
                tesla_response: message,
                source_id: state.source_token,
                log_event_id: event_id
              )

              {:reply, :error, %{state | next_update: next_update()}}
          end
      end
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:update, schema, type_map, field_count}, _from, state) do
    Sources.Cache.put_bq_schema(state.source_token, schema)

    {:reply, :ok,
     %{
       state
       | schema: schema,
         type_map: type_map,
         field_count: field_count,
         next_update: next_update()
     }}
  end

  def handle_info(:persist, state) do
    source = Sources.Cache.get_by(token: state.source_token)

    Sources.create_or_update_source_schema(source, %{bigquery_schema: state.schema})

    persist()

    {:noreply, state}
  end

  defp persist(persist_every \\ @persist_every) do
    Process.send_after(self(), :persist, persist_every)
  end

  defp count_fields(type_map) do
    type_map
    |> Iteraptor.to_flatmap()
    |> Enum.count()
  end

  defp next_update() do
    System.system_time(:second) + 60
  end

  defp name(source_token) do
    String.to_atom("#{source_token}" <> "-schema")
  end

  defp same_schemas?(old_schema, new_schema) do
    old_schema == new_schema
  end
end
