defmodule Logflare.Source.BigQuery.Schema do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.Cluster
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.BigQuery.GenUtils
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources
  alias Logflare.LogEvent

  @persist_every 60_000

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
    case BigQuery.get_table(state.source_token) do
      {:ok, table} ->
        schema = SchemaUtils.deep_sort_by_fields_name(table.schema)
        type_map = SchemaUtils.to_typemap(schema)
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

      {:error, response} ->
        Sources.Cache.put_bq_schema(state.source_token, state.schema)

        Logger.info(
          "Schema manager init error: #{state.source_token}: #{
            BigQuery.GenUtils.get_tesla_error_message(response)
          } "
        )

        {:noreply, %{state | next_update: next_update()}}
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
    nodes = Cluster.Utils.node_list_all()

    GenServer.multi_call(nodes, name(source_token), {:update, log_event})
  end

  def handle_call(:get, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:update, %LogEvent{body: body, id: event_id}}, _from, state) do
    schema = SchemaBuilder.build_table_schema(body.metadata, state.schema)

    if not same_schemas?(state.schema, schema) and state.next_update < System.system_time(:second) and
         state.field_count < 500 do
      case BigQuery.patch_table(
             state.source_id,
             schema,
             state.bigquery_dataset_id,
             state.bigquery_project_id
           ) do
        {:ok, table_info} ->
          new_schema =
            table_info.schema
            |> SchemaUtils.deep_sort_by_fields_name()

          type_map = SchemaUtils.to_typemap(new_schema)
          field_count = count_fields(type_map)

          Logger.info("Source schema updated!",
            source_id: state.source_id,
            log_event_id: event_id
          )

          {:reply, :ok,
           %{
             state
             | schema: new_schema,
               type_map: type_map,
               field_count: field_count
           }}

        {:error, response} ->
          Logger.warn("Source schema update error!",
            tesla_response: GenUtils.get_tesla_error_message(response),
            source_id: state.source_id,
            log_event_id: event_id
          )

          {:reply, :error, %{state | next_update: next_update()}}
      end
    end
  end

  def handle_call({:update, schema}, _from, state) do
    sorted = SchemaUtils.deep_sort_by_fields_name(schema)
    type_map = SchemaUtils.to_typemap(sorted)
    field_count = count_fields(type_map)

    Sources.Cache.put_bq_schema(state.source_token, sorted)

    {:reply, :ok,
     %{
       state
       | schema: sorted,
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
