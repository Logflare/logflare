defmodule Logflare.TableBigQueryPipeline do
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Google.BigQuery
  alias GoogleApi.BigQuery.V2.Model
  alias Logflare.TableBigQuerySchema

  def start_link(website_table) do
    Broadway.start_link(__MODULE__,
      name: name(website_table),
      producers: [
        ets: [
          module: {BroadwayBuffer.Producer, table_name: website_table, config: []}
        ]
      ],
      processors: [
        default: [stages: 5]
      ],
      batchers: [
        bq: [stages: 5, batch_size: 100, batch_timeout: 1000]
      ]
    )
  end

  def handle_message(_processor_name, message, _context) do
    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:bq)
  end

  def handle_batch(:bq, messages, _batch_info, _context) do
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
                "timestamp" => bq_timestamp,
                "event_message" => payload.log_message,
                "metadata" => [payload.metadata]
              }
          end

        %Model.TableDataInsertAllRequestRows{
          insertId: Ecto.UUID.generate(),
          json: row_json
        }
      end)

    table_atom = get_table(messages)
    BigQuery.stream_batch!(table_atom, rows)

    messages
  end

  defp process_data(message) do
    %{event: {_time_event, payload}, table: table} = message

    case Map.has_key?(payload, :metadata) do
      true ->
        old_schema = TableBigQuerySchema.get(table)
        schema = build_schema(payload.metadata, old_schema)

        if same_schemas?(old_schema, schema) == false do
          case BigQuery.patch_table(table, schema) do
            {:ok, table_info} ->
              TableBigQuerySchema.update(table, table_info.schema)
              Logger.info("Table schema updated!")

            {:error, _message} ->
              Logger.error("Table schema error!")
          end
        end

        message

      false ->
        message
    end
  end

  defp get_table(messages) do
    message = Enum.at(messages, 0)
    {_module, table, _unsuccessful} = message.acknowledger
    table
  end

  defp name(website_table) do
    String.to_atom("#{website_table}" <> "-pipeline")
  end

  defp build_schema(metadata, old_schema) do
    %Model.TableSchema{
      fields: [
        %Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %Model.TableFieldSchema{
          description: nil,
          fields: nil,
          mode: nil,
          name: "event_message",
          type: "STRING"
        },
        %Model.TableFieldSchema{
          description: nil,
          mode: "REPEATED",
          name: "metadata",
          type: "RECORD",
          fields: build_fields(metadata, old_schema)
        }
      ]
    }
  end

  defp build_fields(metadata, old_schema) do
    new_fields =
      Enum.map(metadata, fn {k, v} ->
        build_field(k, v)
      end)

    old_record = Enum.find(old_schema.fields, fn x -> x.name == "metadata" end)
    old_fields = old_record.fields

    Enum.uniq(new_fields ++ old_fields)
  end

  defp build_field(key, value) do
    type = check_type(value)

    %Model.TableFieldSchema{
      name: key,
      type: type,
      mode: "NULLABLE"
    }
  end

  defp same_schemas?(old_schema, new_schema) do
    old_record = Enum.find(old_schema.fields, fn x -> x.name == "metadata" end)
    new_record = Enum.find(new_schema.fields, fn x -> x.name == "metadata" end)

    case is_nil(old_record) do
      true ->
        false

      false ->
        old_fields = Enum.sort_by(old_record.fields, fn f -> f.name end)
        new_fields = Enum.sort_by(new_record.fields, fn f -> f.name end)

        old_fields == new_fields
    end
  end

  defp check_type(value) when is_map(value), do: "RECORD"
  defp check_type(value) when is_integer(value), do: "INTEGER"
  defp check_type(value) when is_binary(value), do: "STRING"
  defp check_type(value) when is_boolean(value), do: "BOOLEAN"
end
