defmodule Logflare.Source.BigQuery.Pipeline do
  @moduledoc false
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Source.BigQuery.{Schema, SchemaBuilder, BufferProducer}
  alias Logflare.Google.BigQuery.{GenUtils, EventUtils}
  alias Logflare.{Source}
  alias Logflare.{Users, Sources}
  alias Logflare.Source.Supervisor
  alias Logflare.{AccountEmail, Mailer}
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    Broadway.start_link(__MODULE__,
      name: name(source_id),
      producer: [
        module: {BufferProducer, rls},
        hibernate_after: 30_000
      ],
      processors: [
        default: [stages: 5]
      ],
      batchers: [
        bq: [stages: 5, batch_size: 100, batch_timeout: 1000]
      ],
      context: rls
    )
  end

  @spec handle_message(any, Broadway.Message.t(), any) :: Broadway.Message.t()
  def handle_message(_processor_name, message, _context) do
    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:bq)
  end

  @spec handle_batch(:bq, list(Broadway.Message.t()), any, RLS.t()) :: [Broadway.Message.t()]
  def handle_batch(:bq, messages, _batch_info, %RLS{} = context) do
    stream_batch(context, messages)
  end

  def le_messages_to_bq_rows(messages) do
    Enum.map(messages, fn message ->
      le_to_bq_row(message.data)
    end)
  end

  def le_to_bq_row(%LE{body: body, id: id}) do
    {:ok, bq_timestamp} = DateTime.from_unix(body.timestamp, :microsecond)

    json = %{
      "timestamp" => bq_timestamp,
      "event_message" => body.message,
      "id" => id
    }

    json =
      if map_size(body.metadata) > 0 do
        metadata = EventUtils.prepare_for_ingest(body.metadata)
        Map.put(json, "metadata", metadata)
      else
        json
      end

    %Model.TableDataInsertAllRequestRows{
      insertId: id,
      json: json
    }
  end

  def stream_batch(%RLS{source_id: source_id} = context, messages) do
    rows = le_messages_to_bq_rows(messages)

    # TODO ... Send some errors through the pipeline again. The generic "retry" error specifically.
    # All others send to the rejected list with the message from BigQuery.
    # See todo in `process_data` also.

    case BigQuery.stream_batch!(context, rows) do
      {:ok, _response} ->
        messages

      {:error, %Tesla.Env{} = response} ->
        case GenUtils.get_tesla_error_message(response) do
          "Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier" =
              message ->
            disconnect_backend_and_email(source_id, message)
            messages

          "The project" <> _tail = message ->
            # "The project web-wtc-1537199112807 has not enabled BigQuery."
            disconnect_backend_and_email(source_id, message)
            messages

          "Not found:" <> _tail = message ->
            disconnect_backend_and_email(source_id, message)
            messages

          _message ->
            Logger.warn("Stream batch response error!",
              tesla_response: GenUtils.get_tesla_error_message(response),
              source_id: source_id
            )

            messages
        end

      {:error, :emfile = response} ->
        Logger.error("Stream batch emfile error!", tesla_response: response)
        messages

      {:error, :timeout = response} ->
        Logger.warn("Stream batch timeout error!", tesla_response: response)
        messages

      {:error, :checkout_timeout = response} ->
        Logger.warn("Stream batch checkout_timeout error!", tesla_response: response)
        messages

      {:error, response} ->
        Logger.warn("Stream batch unknown error!", tesla_response: response)
        messages
    end
  end

  defp process_data(%LE{body: body, source: %Source{token: source_id}, id: event_id} = log_event) do
    schema_state = Schema.get_state(source_id)
    field_count = schema_state.field_count

    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.

    if map_size(body.metadata) > 0 and
         field_count < 500 and
         schema_state.next_update < System.system_time(:second) do
      # TODO Maybe wrap this whole thing in the schema genserver and call it so schema updates are serial
      old_schema = schema_state.schema
      bigquery_project_id = schema_state.bigquery_project_id
      bigquery_dataset_id = schema_state.bigquery_dataset_id

      Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
        try do
          schema = SchemaBuilder.build_table_schema(body.metadata, old_schema)

          if not same_schemas?(old_schema, schema) do
            case BigQuery.patch_table(
                   source_id,
                   schema,
                   bigquery_dataset_id,
                   bigquery_project_id
                 ) do
              {:ok, table_info} ->
                Schema.update(source_id, table_info.schema)

                Logger.info("Source schema updated!",
                  source_id: source_id,
                  log_event_id: event_id
                )

              {:error, response} ->
                Schema.set_next_update(source_id)

                Logger.warn("Source schema update error!",
                  tesla_response: GenUtils.get_tesla_error_message(response),
                  source_id: source_id,
                  log_event_id: event_id
                )
            end
          end
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
        end
      end)

      log_event
    else
      log_event
    end
  end

  defp name(source_id) when is_atom(source_id) do
    String.to_atom("#{source_id}" <> "-pipeline")
  end

  defp same_schemas?(old_schema, new_schema) do
    old_schema == new_schema
  end

  defp disconnect_backend_and_email(source_id, message) when is_atom(source_id) do
    source = Sources.Cache.get_by(token: source_id)
    user = Users.Cache.get_by(id: source.user_id)

    defaults = %{
      bigquery_dataset_location: nil,
      bigquery_project_id: nil,
      bigquery_dataset_id: nil,
      bigquery_processed_bytes_limit: 10_000_000_000
    }

    case Users.update_user_allowed(user, defaults) do
      {:ok, user} ->
        Supervisor.reset_all_user_sources(user)

        AccountEmail.backend_disconnected(user, message)
        |> Mailer.deliver()

        Logger.warn("Backend disconnected for: #{user.email}",
          tesla_response: message
        )

      {:error, changeset} ->
        Logger.error("Failed to reset backend for user: #{user.email}",
          changeset: inspect(changeset)
        )
    end
  end
end
