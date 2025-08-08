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
  alias Logflare.TeamUsers
  alias Logflare.SingleTenant

  def start_link(args) when is_list(args) do
    {name, args} = Keyword.pop(args, :name)

    GenServer.start_link(__MODULE__, args, name: name, hibernate_after: 5_000)
  end

  def init(args) do
    source = Keyword.get(args, :source)

    if source == nil do
      raise ":source must be provided on startup for Schema module"
    end

    # TODO: remove source_id from metadata to reduce confusion
    Logger.metadata(source_id: args[:source_token], source_token: args[:source_token])
    Process.flag(:trap_exit, true)

    {:ok,
     %{
       source_id: source.id,
       source_token: source.token,
       bigquery_project_id: args[:bigquery_project_id],
       bigquery_dataset_id: args[:bigquery_dataset_id],
       field_count: 3,
       field_count_limit: args[:plan].limit_source_fields_limit,
       next_update: System.system_time(:millisecond)
     }, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    if source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: state.source_id) do
      schema = BigQuery.SchemaUtils.deep_sort_by_fields_name(source_schema.bigquery_schema)
      field_count = count_fields(schema)

      {:noreply, %{state | field_count: field_count}}
    else
      {:noreply, state}
    end
  end

  @spec update(atom(), LogEvent.t()) :: :ok
  def update(pid, %LogEvent{} = log_event) when is_pid(pid) or is_tuple(pid) do
    GenServer.cast(pid, {:update, log_event})
  end

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

    source_schema =
      SourceSchemas.Cache.get_source_schema_by(source_id: state.source_id)

    db_schema =
      if source_schema,
        do: source_schema.bigquery_schema,
        else: SchemaBuilder.initial_table_schema()

    schema = try_schema_update(body, db_schema)

    if not same_schemas?(db_schema, schema) and
         state.next_update <= System.system_time(:millisecond) and
         !SingleTenant.postgres_backend?() do
      case BigQuery.patch_table(
             state.source_token,
             schema,
             state.bigquery_dataset_id,
             state.bigquery_project_id
           ) do
        {:ok, _table_info} ->
          field_count = count_fields(schema)

          persist(state.source_id, schema)

          notify_maybe(state.source_token, schema, db_schema)

          {:noreply,
           %{
             state
             | field_count: field_count,
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
                      field_count = count_fields(schema)

                      persist(state.source_id, schema)

                      {:noreply, %{state | field_count: field_count, next_update: next_update()}}

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

  defp persist(source_id, new_schema) do
    source = Sources.Cache.get_by(id: source_id)

    flat_map =
      SchemaUtils.bq_schema_to_flat_typemap(new_schema)

    SourceSchemas.create_or_update_source_schema(source, %{
      bigquery_schema: new_schema,
      schema_flat_map: flat_map
    })
  end

  defp count_fields(schema) do
    schema
    |> BigQuery.SchemaUtils.to_typemap()
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
    old_flatmap = SchemaUtils.bq_schema_to_flat_typemap(old_schema)
    new_flatmap = SchemaUtils.bq_schema_to_flat_typemap(new_schema)

    diff_keys = Map.keys(new_flatmap) -- Map.keys(old_flatmap)

    old_schema == new_schema and Enum.empty?(diff_keys)
  end

  defp try_schema_update(body, schema) do
    SchemaBuilder.build_table_schema(body, schema)
  rescue
    e ->
      Logger.warning("Field schema type change error!", error_string: inspect(e))

      schema
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
