defmodule Logflare.Source.BigQuery.Schema do
  @moduledoc """
  Manages the source schema across a cluster.

  Schema updates are limited to @updates_per_minute. Server is booted with schema from Postgres. Handles schema mismatch between BigQuery and Logflare.
  """
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Source
  alias Logflare.LogEvent
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Google.BigQuery.SchemaUtils

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
        field_count_limit: rls.plan.limit_source_fields_limit,
        next_update: System.system_time(:millisecond)
      },
      name: Source.Supervisor.via(__MODULE__, rls.source_id)
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    persist(0)

    {:ok, state, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    source = Sources.get_by(token: state.source_token)

    case SourceSchemas.get_source_schema_by(source_id: source.id) do
      nil ->
        # persist()

        Logger.info("Nil schema: #{state.source_token}")

        {:noreply, %{state | next_update: next_update()}}

      source_schema ->
        schema = BigQuery.SchemaUtils.deep_sort_by_fields_name(source_schema.bigquery_schema)
        type_map = BigQuery.SchemaUtils.to_typemap(schema)
        field_count = count_fields(type_map)

        # persist()

        {:noreply,
         %{
           state
           | schema: schema,
             type_map: type_map,
             field_count: field_count
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

  def get_state(source_token) when is_atom(source_token) do
    {:ok, pid} = Source.Supervisor.lookup(__MODULE__, source_token)
    GenServer.call(pid, :get)
  end

  def update(source_token, %LogEvent{} = log_event) when is_atom(source_token) do
    {:ok, pid} = Source.Supervisor.lookup(__MODULE__, source_token)
    GenServer.call(pid, {:update, log_event}, @timeout)
  end

  # For tests
  def update(source_token, schema) when is_atom(source_token) do
    {:ok, pid} = Source.Supervisor.lookup(__MODULE__, source_token)
    GenServer.call(pid, {:update, schema}, @timeout)
  end

  # @spec update_cluster(atom(), map(), map(), non_neg_integer()) :: atom
  # def update_cluster(source_token, schema, type_map, field_count) when is_atom(source_token) do
  #  GenServer.abcast(
  #    Node.list(),
  #    pid(source_token),
  #    {:update, schema, type_map, field_count}
  #  )
  # end

  def handle_call(:get, _from, state) do
    {:reply, state, state}
  end

  def handle_call(
        {:update, %LogEvent{}},
        _from,
        %{field_count: fc, field_count_limit: limit} = state
      )
      when fc > limit,
      do: {:reply, :ok, state}

  def handle_call({:update, %LogEvent{body: body, id: event_id}}, _from, state) do
    LogflareLogger.context(source_id: state.source_token, log_event_id: event_id)

    schema = try_schema_update(body, state.schema)

    if not same_schemas?(state.schema, schema) and
         state.next_update < System.system_time(:millisecond) do
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

          Logger.info("Source schema updated from log_event!")

          persist()

          notify_maybe(state.source_token, schema, state.schema)

          {:reply, {:ok, :updated},
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
                  schema = try_schema_update(body, table.schema)

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

                      Logger.info("Source schema updated from BigQuery!")

                      persist()

                      {:reply, {:ok, :updated},
                       %{
                         state
                         | schema: schema,
                           type_map: type_map,
                           field_count: field_count,
                           next_update: next_update()
                       }}

                    {:error, response} ->
                      Logger.warning("Source schema update error!",
                        tesla_response: BigQuery.GenUtils.get_tesla_error_message(response)
                      )

                      {:reply, :error, %{state | next_update: next_update()}}
                  end

                {:error, response} ->
                  Logger.warning("Source schema update error!",
                    tesla_response: BigQuery.GenUtils.get_tesla_error_message(response)
                  )

                  {:reply, :error, %{state | next_update: next_update()}}
              end

            message ->
              Logger.warning("Source schema update error!",
                tesla_response: message
              )

              {:reply, :error, %{state | next_update: next_update()}}
          end
      end
    else
      {:reply, {:ok, :noop}, state}
    end
  end

  def handle_call({:update, schema}, _from, state) do
    sorted = BigQuery.SchemaUtils.deep_sort_by_fields_name(schema)
    type_map = BigQuery.SchemaUtils.to_typemap(sorted)
    field_count = count_fields(type_map)

    persist()

    {:reply, :ok,
     %{
       state
       | schema: sorted,
         type_map: type_map,
         field_count: field_count,
         next_update: next_update()
     }}
  end

  def handle_cast({:update, schema, type_map, field_count}, state) do
    send(self(), :persist)

    {:noreply,
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
    flat_map = SchemaUtils.bq_schema_to_flat_typemap(state.schema)

    SourceSchemas.create_or_update_source_schema(source, %{
      bigquery_schema: state.schema,
      schema_flat_map: flat_map
    })

    # persist()

    {:noreply, state}
  end

  defp persist(persist_every \\ 0) do
    Process.send_after(self(), :persist, persist_every)
  end

  defp count_fields(type_map) do
    type_map
    |> BigQuery.SchemaUtils.flatten_typemap()
    |> Enum.count()
  end

  def next_update_ts(max_updates_per_min) do
    ms = 60 * 1000 / max_updates_per_min
    System.system_time(:millisecond) + ms
  end

  defp next_update() do
    updates_per_minute =
      case Application.get_env(:logflare, :env) do
        :test -> 600
        _ -> 6
      end

    next_update_ts(updates_per_minute)
  end

  defp same_schemas?(old_schema, new_schema) do
    old_schema == new_schema
  end

  defp try_schema_update(body, schema) do
    try do
      SchemaBuilder.build_table_schema(body, schema)
    rescue
      e ->
        # TODO: Put the original log event string JSON into a top level error column with id, timestamp, and metadata
        # This may be a great way to handle type mismatches in general because you get all the other fields anyways.
        # TODO: Render error column somewhere on log event popup

        # And/or put these log events directly into the rejected events list w/ a link to the log event popup.

        LogflareLogger.context(%{
          pipeline_process_data_stacktrace: LogflareLogger.Stacktrace.format(__STACKTRACE__)
        })

        Logger.warning("Field schema type change error!", error_string: inspect(e))

        schema
    end
  end

  defp notify_maybe(source_token, new_schema, old_schema) do
    %Source{user: user} = source = Sources.get_by_and_preload(token: source_token)

    if source.notifications.user_schema_update_notifications do
      AccountEmail.schema_updated(user, source, new_schema, old_schema)
      |> Mailer.deliver()
    end

    for id <- source.notifications.team_user_ids_for_schema_updates do
      team_user = Logflare.TeamUsers.get_team_user(id)

      AccountEmail.schema_updated(team_user, source, new_schema, old_schema)
      |> Mailer.deliver()
    end
  end
end
