defmodule Logflare.Source.BigQuery.Pipeline do
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Source.BigQuery.{Schema, SchemaBuilder, BufferProducer}
  alias Logflare.Google.BigQuery.{GenUtils, EventUtils}

  def start_link(state) do
    Broadway.start_link(__MODULE__,
      name: name(state[:source_token]),
      producers: [
        ets: [
          module: {BufferProducer, table_name: state[:source_token], config: []}
        ]
      ],
      processors: [
        default: [stages: 5]
      ],
      batchers: [
        bq: [stages: 5, batch_size: 100, batch_timeout: 1000]
      ],
      context: state
    )
  end

  def handle_message(_processor_name, message, _context) do
    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:bq)
  end

  def handle_batch(:bq, messages, _batch_info, context) do
    rows =
      Enum.map(messages, fn message ->
        %{event: {_time_event, payload}, table: _table} = message.data
        {:ok, bq_timestamp} = DateTime.from_unix(payload.timestamp, :microsecond)

        row_json =
          case Map.has_key?(payload, :metadata) do
            false ->
              %{
                "timestamp" => bq_timestamp,
                "event_message" => payload.log_message
              }

            true ->
              %{
                "event_message" => payload.log_message,
                "metadata" => EventUtils.prepare_for_injest(payload.metadata),
                "timestamp" => bq_timestamp
              }
          end

        %Model.TableDataInsertAllRequestRows{
          insertId: Ecto.UUID.generate(),
          json: row_json
        }
      end)

    case BigQuery.stream_batch!(context[:source_token], rows, context[:bigquery_project_id]) do
      {:ok, _response} ->
        messages

      {:error, response} ->
        LogflareLogger.merge_context(source_id: context[:source_token])
        LogflareLogger.merge_context(tesla_response: GenUtils.get_tesla_error_message(response))
        Logger.error("Stream batch error!")
        messages
    end
  end

  defp process_data(message) do
    %{event: {time_event, payload}, table: table} = message

    case Map.has_key?(payload, :metadata) do
      true ->
        schema_state = Schema.get_state(table)
        old_schema = schema_state.schema
        bigquery_project_id = schema_state.bigquery_project_id

        try do
          schema = SchemaBuilder.build_table_schema(payload.metadata, old_schema)

          if same_schemas?(old_schema, schema) == false do
            case BigQuery.patch_table(table, schema, bigquery_project_id) do
              {:ok, table_info} ->
                Schema.update(table, table_info.schema)
                Logger.info("Source schema updated!")

              {:error, response} ->
                LogflareLogger.merge_context(
                  tesla_response: GenUtils.get_tesla_error_message(response)
                )

                Logger.error("Source schema update error!")
            end
          end

          message
        rescue
          _e ->
            err = "Field schema type change error!"

            Logger.error(err)

            new_payload = %{payload | metadata: %{"error" => err}}

            Map.put(message, :event, {time_event, new_payload})
        end

      false ->
        message
    end
  end

  defp name(source_id) when is_atom(source_id) do
    String.to_atom("#{source_id}" <> "-pipeline")
  end

  defp same_schemas?(old_schema, new_schema) do
    old_schema == new_schema
  end
end
