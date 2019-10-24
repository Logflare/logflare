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
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    Broadway.start_link(__MODULE__,
      name: name(source_id),
      producers: [
        ets: [
          module: {BufferProducer, rls}
        ]
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

  @spec handle_batch(:bq, list(Broadway.Message.t()), any, RLS.t()) :: any
  def handle_batch(:bq, messages, _batch_info, %RLS{} = context) do
    hackney_stats = :hackney_pool.get_stats(Client.BigQuery)
    LogflareLogger.context(hackney_stats: hackney_stats, source_id: context.source_id)
    stream_batch(context.source_id, messages)
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
      "event_message" => body.message
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

  def stream_batch(source_id, messages) do
    rows = le_messages_to_bq_rows(messages)

    # TODO ... Send some errors through the pipeline again. The generic "retry" error specifically.
    # All others send to the rejected list with the message from BigQuery.
    # See todo in `process_data` also.

    case BigQuery.stream_batch!(source_id, rows) do
      {:ok, _response} ->
        messages

      {:error, %Tesla.Env{} = response} ->
        LogflareLogger.context(tesla_response: GenUtils.get_tesla_error_message(response))
        Logger.warn("Stream batch response error!")
        messages

      {:error, :emfile = response} ->
        LogflareLogger.context(tesla_response: response)
        Logger.error("Stream batch emfile error!")
        messages

      {:error, :timeout = response} ->
        LogflareLogger.context(tesla_response: response)
        Logger.warn("Stream batch timeout error!")
        messages

      {:error, :checkout_timeout = response} ->
        LogflareLogger.context(tesla_response: response)
        Logger.warn("Stream batch checkout_timeout error!")
        messages

      {:error, response} ->
        LogflareLogger.context(tesla_response: response)
        Logger.warn("Stream batch unknown error!")
        messages
    end
  end

  defp process_data(%LE{body: body, source: %Source{token: source_id}} = log_event) do
    LogflareLogger.context(source_id: source_id)
    schema_state = Schema.get_state(source_id)
    field_count = schema_state.field_count

    # TODO ... We use `ignoreUnknownValues: true` when we do `stream_batch!`. If we set that to `true`
    # then this makes BigQuery check the payloads for new fields. In the response we'll get a list of events that didn't validate.
    # Send those events through the pipeline again, but run them through our schema process this time. Do all
    # these things a max of like 5 times and after that send them to the rejected pile.

    if map_size(body.metadata) > 0 do
      if field_count < 500 do
        old_schema = schema_state.schema
        bigquery_project_id = schema_state.bigquery_project_id
        bigquery_dataset_id = schema_state.bigquery_dataset_id

        try do
          schema = SchemaBuilder.build_table_schema(body.metadata, old_schema)

          if same_schemas?(old_schema, schema) == false do
            hackney_stats = :hackney_pool.get_stats(Client.BigQuery)
            LogflareLogger.context(hackney_stats: hackney_stats)

            case BigQuery.patch_table(source_id, schema, bigquery_dataset_id, bigquery_project_id) do
              {:ok, table_info} ->
                Schema.update(source_id, table_info.schema)
                Logger.info("Source schema updated!")

              {:error, response} ->
                LogflareLogger.context(tesla_response: GenUtils.get_tesla_error_message(response))
                Logger.warn("Source schema update error!")
            end
          end

          log_event
        rescue
          _e ->
            err = "Field schema type change error!"

            new_body = %{body | metadata: %{"error" => err}}

            Logger.warn(err)

            %{log_event | body: new_body}
        end
      else
        log_event
      end
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
end
