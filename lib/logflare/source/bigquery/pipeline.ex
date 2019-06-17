defmodule Logflare.Source.BigQuery.Pipeline do
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.Source.BigQuery.{Schema, SchemaBuilder, BufferProducer}
  alias Logflare.Google.BigQuery.{GenUtils, EventUtils}
  alias Logflare.Sources
  alias Logflare.LogEvent, as: LE

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
    LogflareLogger.merge_context(source_id: context[:source_token])

    rows =
      Enum.map(messages, fn message ->
        %{event: %LE{body: body}, table: _table} = message.data
        {:ok, bq_timestamp} = DateTime.from_unix(body.timestamp, :microsecond)

        row_json =
          if map_size(body.metadata) > 0 do
            %{
              "event_message" => body.message,
              "metadata" => EventUtils.prepare_for_injest(body.metadata),
              "timestamp" => bq_timestamp
            }
          else
            %{
              "timestamp" => bq_timestamp,
              "event_message" => body.message
            }
          end

        %Model.TableDataInsertAllRequestRows{
          insertId: Ecto.UUID.generate(),
          json: row_json
        }
      end)

    hackney_stats = :hackney_pool.get_stats(Client.BigQuery)
    LogflareLogger.merge_context(hackney_stats: hackney_stats)

    case BigQuery.stream_batch!(context[:source_token], rows, context[:bigquery_project_id]) do
      {:ok, _response} ->
        messages

      {:error, %Tesla.Env{} = response} ->
        LogflareLogger.merge_context(tesla_response: GenUtils.get_tesla_error_message(response))
        Logger.error("Stream batch response error!")
        messages

      {:error, :emfile = response} ->
        LogflareLogger.merge_context(tesla_response: response)
        Logger.error("Stream batch emfile error!")
        messages

      {:error, :timeout = response} ->
        LogflareLogger.merge_context(tesla_response: response)
        Logger.error("Stream batch timeout error!")
        messages
    end
  end

  defp process_data(message) do
    %{event: %LE{body: body} = log_event, table: table} = message

    LogflareLogger.merge_context(source_id: table)

    if map_size(body) > 0 do
      schema_state = Schema.get_state(table)
      old_schema = schema_state.schema
      bigquery_project_id = schema_state.bigquery_project_id

      try do
        schema = SchemaBuilder.build_table_schema(body.metadata, old_schema)

        if same_schemas?(old_schema, schema) == false do
          hackney_stats = :hackney_pool.get_stats(Client.BigQuery)
          LogflareLogger.merge_context(hackney_stats: hackney_stats)

          case BigQuery.patch_table(table, schema, bigquery_project_id) do
            {:ok, table_info} ->
              Schema.update(table, table_info.schema)
              Sources.Cache.put_bq_schema(table, table_info.schema)
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

          new_body = %{body | metadata: %{"error" => err}}

          Logger.error(err)
          Map.put(message, :event, %{log_event | body: new_body})
      end
    else
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
