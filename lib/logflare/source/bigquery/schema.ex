defmodule Logflare.Source.BigQuery.Schema do
  @moduledoc """
  Manages the source schema across a cluster.

  Schemas should only be updated once per minute. Server is booted with schema from Postgres. Handles schema mismatch between BigQuery and Logflare.
  """
  use GenServer
  use TypedStruct

  require Logger

  use Logflare.Commons
  alias Logflare.Google.BigQuery
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  @persist_every_seconds 60
  @timeout 60_000

  typedstruct do
    field :source_token, atom()
    field :bigquery_project_id, String.t()
    field :bigquery_dataset_id, String.t()
    field :field_count_limit, integer()
    field :schema, map()
    field :next_update, integer()
  end

  def start_link(%RLS{} = rls) do
    GenServer.start_link(
      __MODULE__,
      %__MODULE__{
        source_token: rls.source_id,
        bigquery_project_id: rls.bigquery_project_id,
        bigquery_dataset_id: rls.bigquery_dataset_id,
        field_count_limit: rls.plan.limit_source_fields_limit
      },
      name: name(rls.source_id)
    )
  end

  def init(state) do
    Process.flag(:trap_exit, true)

    {:ok, state, {:continue, :boot}}
  end

  def handle_continue(:boot, state) do
    {:noreply, state}
  end

  def terminate(reason, %__MODULE__{} = state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{
      source_id: state.source_token
    })

    reason
  end

  def get_state(source_token) do
    GenServer.call(name(source_token), :get)
  end

  @spec update(atom(), TFS.t() | LE.t()) :: {:ok, term} | {:error, term}
  def update(source_token, %LE{} = log_event) when is_atom(source_token) do
    GenServer.call(name(source_token), {:update, log_event}, @timeout)
  end

  def update(source_token, %TFS{} = schema) when is_atom(source_token) do
    GenServer.call(name(source_token), {:update, schema}, @timeout)
  end

  def handle_call(:get, _from, %__MODULE__{} = state) do
    {:reply, state, state}
  end

  def handle_call(
        {:update, %LE{}},
        _from,
        %{field_count: fc, field_count_limit: limit} = state
      )
      when fc > limit do
    {:reply, :ok, state}
  end

  def handle_call(
        {:update, %LE{body: body, id: event_id} = le},
        _from,
        %__MODULE__{} = state
      ) do
    LogflareLogger.context(source_id: state.source_token, log_event_id: event_id)

    %{bigquery_schema: current_bigquery_schema, updated_at: updated_at} =
      SourceSchemas.get_source_schema_by(source_id: le.source.id)

    source = Sources.get_source!(state.source_token)

    maybe_new_schema = try_schema_update(body.metadata, current_bigquery_schema)
    same_schema? = same_schemas?(current_bigquery_schema, maybe_new_schema)

    next_update_time_reached? =
      Timex.diff(Timex.now(), updated_at, :seconds) >= @persist_every_seconds

    if not same_schema? and next_update_time_reached? do
      case BigQuery.patch_table(
             state.source_token,
             maybe_new_schema,
             state.bigquery_dataset_id,
             state.bigquery_project_id
           ) do
        {:ok, _table_info} ->
          Logger.info("Source schema updated from log_event!")

          SourceSchemas.update_source_schema_for_source(
            %{bigquery_schema: maybe_new_schema},
            source
          )

          notify_maybe(state.source_token, maybe_new_schema, current_bigquery_schema)

          {:reply, :ok, state}

        {:error, response} ->
          case BigQuery.GenUtils.get_tesla_error_message(response) do
            "Provided Schema does not match Table" <> _tail = _message ->
              case BigQuery.get_table(state.source_token) do
                {:ok, table} ->
                  schema = try_schema_update(body.metadata, table.schema)

                  case BigQuery.patch_table(
                         state.source_token,
                         schema,
                         state.bigquery_dataset_id,
                         state.bigquery_project_id
                       ) do
                    {:ok, _table_info} ->
                      Logger.info("Source schema updated from BigQuery!")

                      SourceSchemas.update_source_schema_for_source(
                        %{bigquery_schema: schema},
                        source
                      )

                      {:reply, :ok, state}

                    {:error, response} ->
                      Logger.warn("Source schema update error!",
                        tesla_response: BigQuery.GenUtils.get_tesla_error_message(response)
                      )

                      {:reply, :error, state}
                  end

                {:error, response} ->
                  Logger.warn("Source schema update error!",
                    tesla_response: BigQuery.GenUtils.get_tesla_error_message(response)
                  )

                  {:reply, :error, state}
              end

            message ->
              Logger.warn("Source schema update error!",
                tesla_response: message
              )

              {:reply, :error, state}
          end
      end
    else
      {:reply, :ok, state}
    end
  end

  def handle_call({:update, %TFS{} = schema}, _from, %__MODULE__{} = state) do
    source = Sources.get_source_by(token: state.source_token)
    sorted = BigQuery.SchemaUtils.deep_sort_by_fields_name(schema)

    {:ok, schema} =
      SourceSchemas.update_source_schema_for_source(
        %{bigquery_schema: sorted},
        source
      )

    {:reply, :ok, %{state | schema: schema}}
  end

  def handle_cast({:update, schema, _type_map, _field_count}, %__MODULE__{} = state) do
    {:noreply, %{state | schema: schema}}
  end

  defp name(source_token) do
    String.to_atom("#{source_token}" <> "-schema")
  end

  defp same_schemas?(old_schema, new_schema) do
    old_schema == new_schema
  end

  defp try_schema_update(metadata, schema) do
    try do
      SchemaBuilder.build_table_schema(metadata, schema)
    rescue
      e ->
        # TODO: Put the original log event string JSON into a top level error column with id, timestamp, and metadata
        # This may be a great way to handle type mismatches in general because you get all the other fields anyways.
        # TODO: Render error column somewhere on log event popup

        # And/or put these log events directly into the rejected events list w/ a link to the log event popup.

        LogflareLogger.context(%{
          pipeline_process_data_stacktrace: LogflareLogger.Stacktrace.format(__STACKTRACE__)
        })

        Logger.warn("Field schema type change error!", error_string: inspect(e))

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
