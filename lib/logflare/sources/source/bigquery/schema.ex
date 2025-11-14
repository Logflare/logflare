defmodule Logflare.Sources.Source.BigQuery.Schema do
  @moduledoc """
  Manages the source schema across a cluster.

  Schema updates are limited to `Application.get_env(:logflare, #{__MODULE__})[:updates_per_minute]`.
  Handles schema mismatch between BigQuery and Logflare.
  """
  use GenServer

  require Logger

  alias Logflare.Google.BigQuery
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Sources.Source
  alias Logflare.LogEvent
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.TeamUsers
  alias Logflare.SingleTenant

  def start_link(args) when is_list(args) do
    {name, args} = Keyword.pop(args, :name)

    GenServer.start_link(__MODULE__, args,
      name: name,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 500]
    )
  end

  @spec update(atom(), LogEvent.t(), Source.t()) :: :ok
  def update(pid, %LogEvent{} = log_event, %Source{} = source)
      when is_pid(pid) or is_tuple(pid) do
    GenServer.cast(pid, {:update, log_event, source})
  end

  # GenServer callbacks

  def init(args) do
    %Source{id: source_id, token: source_token} = Keyword.get(args, :source)

    Logger.metadata(source_id: source_id, source_token: source_token)
    Process.flag(:trap_exit, true)

    state = %{
      source_id: source_id,
      source_token: source_token,
      bigquery_project_id: args[:bigquery_project_id],
      bigquery_dataset_id: args[:bigquery_dataset_id],
      field_count: 3,
      field_count_limit: Map.get(args[:plan] || %{}, :limit_source_fields_limit, 500),
      next_update: System.system_time(:millisecond)
    }

    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: source_id)
    {:ok, state, {:continue, {:boot, source_schema}}}
  end

  def handle_continue({:boot, nil}, state), do: {:noreply, state}

  def handle_continue({:boot, source_schema}, state) do
    schema = BigQuery.SchemaUtils.deep_sort_by_fields_name(source_schema.bigquery_schema)
    {:noreply, %{state | field_count: count_fields(schema)}}
  end

  def handle_cast(
        {:update, %LogEvent{}, %Source{lock_schema: true}},
        state
      ),
      do: {:noreply, state}

  def handle_cast(
        {:update, %LogEvent{}, _source},
        %{field_count: fc, field_count_limit: limit} = state
      )
      when fc > limit,
      do: {:noreply, state}

  def handle_cast({:update, %LogEvent{body: body, id: event_id}, _source}, state) do
    LogflareLogger.context(source_id: state.source_token, log_event_id: event_id)

    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: state.source_id)

    db_schema =
      if source_schema,
        do: source_schema.bigquery_schema,
        else: SchemaBuilder.initial_table_schema()

    schema = try_schema_update(body, db_schema)

    if schema_needs_update?(db_schema, schema, state) do
      case patch_bigquery_table(state, schema) do
        {:ok, _table_info} ->
          handle_successful_patch(state, schema, db_schema)

        {:error, response} ->
          handle_patch_error(body, state, response)
      end
    else
      {:noreply, state}
    end
  end

  defp schema_needs_update?(db_schema, schema, state) do
    not same_schemas?(db_schema, schema) and
      state.next_update <= System.system_time(:millisecond) and
      not SingleTenant.postgres_backend?()
  end

  defp patch_bigquery_table(state, schema) do
    BigQuery.patch_table(
      state.source_token,
      schema,
      state.bigquery_dataset_id,
      state.bigquery_project_id
    )
  end

  defp handle_successful_patch(state, schema, db_schema) do
    persist(state.source_id, schema)
    notify_maybe(state.source_token, schema, db_schema)

    {:noreply, %{state | field_count: count_fields(schema), next_update: next_update()}}
  end

  defp handle_patch_error(body, state, response) do
    case BigQuery.GenUtils.get_tesla_error_message(response) do
      "Provided Schema does not match Table" <> _tail ->
        handle_schema_mismatch(body, state)

      message ->
        log_error_and_update_state(body, state, message)
    end
  end

  defp handle_schema_mismatch(body, state) do
    with {:ok, table} <- BigQuery.get_table(state.source_token),
         schema <- try_schema_update(body, table.schema),
         {:ok, _table_info} <- patch_bigquery_table(state, schema) do
      field_count = count_fields(schema)
      persist(state.source_id, schema)
      {:noreply, %{state | field_count: field_count, next_update: next_update()}}
    else
      {:error, response} ->
        error_message = BigQuery.GenUtils.get_tesla_error_message(response)
        log_error_and_update_state(body, state, error_message)
    end
  end

  defp log_error_and_update_state(body, state, error_message) do
    Logger.warning("Source schema update error!",
      error_string: "Sample event: #{inspect(body)}",
      tesla_response: error_message
    )

    {:noreply, %{state | next_update: next_update()}}
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

  defp next_update do
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
