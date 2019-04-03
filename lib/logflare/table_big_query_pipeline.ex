defmodule Logflare.TableBigQueryPipeline do
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.Google.BigQuery

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
        {:value, {_time_event, payload}} = message.data

        {:ok, bq_timestamp} = DateTime.from_unix(payload.timestamp, :microsecond)
        row_json = %{"timestamp" => bq_timestamp, "event_message" => payload.log_message}

        %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows{
          insertId: Ecto.UUID.generate(),
          json: row_json
        }
      end)

    table_atom = get_table(messages)
    BigQuery.stream_batch!(table_atom, rows)

    messages
  end

  defp process_data(data) do
    # Do some calculations, generate a JSON representation, process images.
    data
  end

  defp get_table(messages) do
    message = Enum.at(messages, 0)
    {_module, table, _unsuccessful} = message.acknowledger
    table
  end

  defp name(website_table) do
    String.to_atom("#{website_table}" <> "-pipeline")
  end
end
