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
  alias Logflare.Sources.Source.BigQuery.SchemaMetrics
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Sources
  alias Logflare.SourceSchemas
  alias Logflare.Sources.Source
  alias Logflare.LogEvent
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.TeamUsers
  alias Logflare.SingleTenant

  @sample_counter_scale 1_000_000

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

  @doc false
  def update(pid, %LogEvent{} = log_event, %Source{} = source, reservation)
      when is_pid(pid) and is_tuple(reservation) do
    GenServer.cast(pid, {:update, log_event, source, reservation})
  end

  @doc false
  def reserve_update_slot(sample_counter, limit)
      when is_reference(sample_counter) and is_integer(limit) and limit > 0 and
             limit < @sample_counter_scale do
    current = :atomics.get(sample_counter, 1)
    generation = div(current, @sample_counter_scale)

    if rem(current, @sample_counter_scale) >= limit do
      :full
    else
      case :atomics.compare_exchange(sample_counter, 1, current, current + 1) do
        :ok -> {:ok, {sample_counter, generation}}
        _actual -> reserve_update_slot(sample_counter, limit)
      end
    end
  end

  @doc false
  def pending_update_slots(sample_counter) when is_reference(sample_counter) do
    sample_counter
    |> :atomics.get(1)
    |> rem(@sample_counter_scale)
  end

  @doc false
  def reset_update_slots(sample_counter) when is_reference(sample_counter) do
    current = :atomics.get(sample_counter, 1)
    generation = div(current, @sample_counter_scale) + 1
    :atomics.put(sample_counter, 1, generation * @sample_counter_scale)
    :ok
  end

  @doc false
  def release_update_slot(sample_counter, generation) do
    current = :atomics.get(sample_counter, 1)

    cond do
      div(current, @sample_counter_scale) != generation ->
        :ok

      rem(current, @sample_counter_scale) == 0 ->
        :ok

      :atomics.compare_exchange(sample_counter, 1, current, current - 1) == :ok ->
        :ok

      true ->
        release_update_slot(sample_counter, generation)
    end
  end

  # Public for profiling: benchmarks can target the GenServer's pure schema
  # planning work without measuring mailbox scheduling or BigQuery side effects.
  @doc false
  def plan_update(body, db_schema, state) do
    schema = try_schema_update(body, db_schema)

    if schema_needs_update?(db_schema, schema, state) do
      {:update, schema}
    else
      :noop
    end
  end

  # GenServer callbacks

  def init(args) do
    %Source{id: source_id, token: source_token, user_id: user_id, system_source: system_source} =
      Keyword.get(args, :source)

    Logger.metadata(
      source_id: source_token,
      source_token: source_token,
      user_id: user_id,
      system_source: system_source
    )

    Process.flag(:trap_exit, true)

    if sample_counter = args[:sample_counter] do
      reset_update_slots(sample_counter)
    end

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
        {:update, %LogEvent{} = log_event, %Source{} = source, {sample_counter, generation}},
        state
      ) do
    release_update_slot(sample_counter, generation)

    SchemaMetrics.record_handled()
    handle_update(log_event, source, state)
  end

  def handle_cast({:update, %LogEvent{} = log_event, %Source{} = source}, state) do
    handle_update(log_event, source, state)
  end

  defp handle_update(%LogEvent{}, %Source{lock_schema: true}, state), do: {:noreply, state}

  defp handle_update(
         %LogEvent{},
         _source,
         %{field_count: fc, field_count_limit: limit} = state
       )
       when fc > limit,
       do: {:noreply, state}

  defp handle_update(%LogEvent{body: body, id: event_id}, _source, state) do
    LogflareLogger.context(source_id: state.source_token, log_event_id: event_id)

    source_schema = SourceSchemas.Cache.get_source_schema_by(source_id: state.source_id)

    db_schema =
      if source_schema,
        do: source_schema.bigquery_schema,
        else: SchemaBuilder.initial_table_schema()

    case plan_update(body, db_schema, state) do
      {:update, schema} ->
        case patch_bigquery_table(state, schema) do
          {:ok, _table_info} ->
            handle_successful_patch(state, schema, db_schema)

          {:error, response} ->
            handle_patch_error(body, state, response)
        end

      :noop ->
        {:noreply, state}
    end
  end

  defp schema_needs_update?(db_schema, schema, state) do
    not same_schemas?(db_schema, schema) and
      state.next_update <= System.system_time(:millisecond) and
      not SingleTenant.postgres_backend?()
  end

  defp patch_bigquery_table(state, schema) do
    instrument_phase(:patch, state.source_id, fn ->
      BigQuery.patch_table(
        state.source_token,
        schema,
        state.bigquery_dataset_id,
        state.bigquery_project_id
      )
    end)
  end

  defp handle_successful_patch(state, schema, db_schema) do
    instrument_phase(:persist, state.source_id, fn -> persist(state.source_id, schema) end)

    instrument_phase(:notify, state.source_id, fn ->
      notify_maybe(state.source_token, schema, db_schema)
    end)

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
    with {:ok, table} <-
           instrument_phase(:refresh, state.source_id, fn ->
             BigQuery.get_table(state.source_token)
           end),
         schema <- try_schema_update(body, table.schema),
         {:ok, _table_info} <- patch_bigquery_table(state, schema) do
      field_count = count_fields(schema)
      instrument_phase(:persist, state.source_id, fn -> persist(state.source_id, schema) end)
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
    |> BigQuery.SchemaUtils.to_typemap(from: :bigquery_schema)
    |> BigQuery.SchemaUtils.flatten_typemap()
    |> Enum.count()
  end

  defp instrument_phase(phase, source_id, function) do
    metadata = %{phase: phase, source_id: source_id}

    :telemetry.span([:logflare, :bigquery, :schema, :operation], metadata, fn ->
      result = function.()
      {result, Map.put(metadata, :result, telemetry_result(result))}
    end)
  end

  defp telemetry_result({:error, _reason}), do: :error
  defp telemetry_result(_result), do: :ok

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
