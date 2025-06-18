defmodule Logflare.Source.BigQuery.Schema do
  @moduledoc """
  Manages the source schema across a cluster.

  Schema updates are limited to @updates_per_minute. Server is booted with schema from Postgres. Handles schema mismatch between BigQuery and Logflare.
  """
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Source
  alias Logflare.LogEvent
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Backends
  alias Logflare.TeamUsers
  alias Logflare.SingleTenant

  def start_link(args) when is_list(args) do
    {name, args} = Keyword.pop(args, :name)

    GenServer.start_link(__MODULE__, args,
      name: name,
      spawn_opt: [
        fullsweep_after: 1_000
      ],
      hibernate_after: 5_000
    )
  end

  def init(args) do
    source = Keyword.get(args, :source)

    if source == nil do
      raise ":source must be provided on startup for Schema module"
    end

    # TODO: remove source_id from metadata to reduce confusion
    Logger.metadata(source_id: args[:source_token], source_token: args[:source_token])
    Process.flag(:trap_exit, true)

    persist(0)

    {:ok,
     %{
       source_id: source.id,
       source_token: source.token,
       bigquery_project_id: args[:bigquery_project_id],
       bigquery_dataset_id: args[:bigquery_dataset_id],
       schema: SchemaBuilder.initial_table_schema(),
       type_map: %{
         event_message: %{t: :string},
         timestamp: %{t: :datetime},
         id: %{t: :string}
       },
       field_count: 3,
       field_count_limit: args[:plan].limit_source_fields_limit,
       next_update: System.system_time(:millisecond)
     }, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    case SourceSchemas.Cache.get_source_schema_by(source_id: state.source_id) do
      nil ->
        Logger.info("Nil schema: #{state.source_token}")

        {:noreply, %{state | next_update: next_update()}}

      source_schema ->
        schema = BigQuery.SchemaUtils.deep_sort_by_fields_name(source_schema.bigquery_schema)
        type_map = BigQuery.SchemaUtils.to_typemap(schema)
        field_count = count_fields(type_map)

        {:noreply,
         %{
           state
           | schema: schema,
             type_map: type_map,
             field_count: field_count
         }}
    end
  end

  # TODO: remove, external procs should not have access to internal state
  def get_state(source_token) when is_atom(source_token) do
    with {:ok, pid} <- Backends.lookup(__MODULE__, source_token) do
      GenServer.call(pid, :get)
    end
  end

  # TODO: remove, external procs should not have access to internal state
  def get_state(name), do: GenServer.call(name, :get)

  @spec update(atom(), LogEvent.t()) :: :ok
  def update(pid, %LogEvent{} = log_event) when is_pid(pid) or is_tuple(pid) do
    GenServer.cast(pid, {:update, log_event})
  end

  # TODO: remove, external procs should not have access to internal state
  def handle_call(:get, _from, state), do: {:reply, state, state}

  def handle_cast(
        {:update, %LogEvent{source: %Source{lock_schema: true}}},
        state
      ),
      do: {:noreply, state}

  def handle_cast(
        {:update, %LogEvent{}},
        %{field_count: fc, field_count_limit: limit} = state
      )
      when fc > limit,
      do: {:noreply, state}

  def handle_cast({:update, %LogEvent{body: body, id: event_id}}, state) do
    LogflareLogger.context(source_id: state.source_token, log_event_id: event_id)

    schema = try_schema_update(body, state.schema)

    if not same_schemas?(state.schema, schema) and
         state.next_update <= System.system_time(:millisecond) and
         !SingleTenant.postgres_backend?() do
      case BigQuery.patch_table(
             state.source_token,
             schema,
             state.bigquery_dataset_id,
             state.bigquery_project_id
           ) do
        {:ok, _table_info} ->
          type_map = BigQuery.SchemaUtils.to_typemap(schema)
          field_count = count_fields(type_map)

          Logger.info("Source schema updated from log_event!")

          persist()

          notify_maybe(state.source_token, schema, state.schema)

          {:noreply,
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

                      Logger.info("Source schema updated from BigQuery!")

                      persist()

                      {:noreply,
                       %{
                         state
                         | schema: schema,
                           type_map: type_map,
                           field_count: field_count,
                           next_update: next_update()
                       }}

                    {:error, response} ->
                      Logger.warning("Source schema update error!",
                        error_string: "Sample event: #{inspect(body)}",
                        tesla_response: BigQuery.GenUtils.get_tesla_error_message(response)
                      )

                      {:noreply, %{state | next_update: next_update()}}
                  end

                {:error, response} ->
                  Logger.warning("Source schema update error!",
                    error_string: "Sample event: #{inspect(body)}",
                    tesla_response: BigQuery.GenUtils.get_tesla_error_message(response)
                  )

                  {:noreply, %{state | next_update: next_update()}}
              end

            message ->
              Logger.warning("Source schema update error!",
                error_string: "Sample event: #{inspect(body)}",
                tesla_response: message
              )

              {:noreply, %{state | next_update: next_update()}}
          end
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(:persist, state) do
    source = Sources.Cache.get_by(token: state.source_token)
    flat_map = SchemaUtils.bq_schema_to_flat_typemap(state.schema)

    SourceSchemas.create_or_update_source_schema(source, %{
      bigquery_schema: state.schema,
      schema_flat_map: flat_map
    })

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
    updates_per_minute = Application.get_env(:logflare, __MODULE__)[:updates_per_minute]
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
        Logger.warning("Field schema type change error!", error_string: inspect(e))

        schema
    end
  end

  # public function for testing
  def notify_maybe(source_token, new_schema, old_schema) do
    %Source{user: user} = source = Sources.Cache.get_by_and_preload(token: source_token)

    if source.notifications.user_schema_update_notifications do
      AccountEmail.schema_updated(user, source, new_schema, old_schema)
      |> Mailer.deliver()
    end

    for id <- source.notifications.team_user_ids_for_schema_updates,
        team_user = TeamUsers.Cache.get_team_user(id),
        team_user != nil do
      AccountEmail.schema_updated(team_user, source, new_schema, old_schema)
      |> Mailer.deliver()
    end
  end
end
