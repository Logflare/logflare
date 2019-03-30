defmodule Logflare.BigQueryPipeline do
  use Broadway

  require Logger

  alias Broadway.Message
  alias Logflare.BigQuery

  @table_name "da7660a6-447a-4735-b385-4d6dc8929857"

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producers: [
        ets: [
          module: {BroadwayETS.Producer, table_name: @table_name, config: []}
        ]
      ],
      processors: [
        default: [stages: 5]
      ],
      batchers: [
        bq: [stages: 5, batch_size: 10, batch_timeout: 1000]
      ]
    )
  end

  def handle_message(_processor_name, message, _context) do
    IO.inspect(message, label: "Message handled")

    message
    |> Message.update_data(&process_data/1)
    |> Message.put_batcher(:bq)
  end

  def handle_batch(:bq, messages, _batch_info, _context) do
    rows =
      Enum.map(messages, fn message ->
        {_module, table, _blah} = message.acknowledger
        [{_time_event, payload}] = message.data

        {:ok, bq_timestamp} = DateTime.from_unix(payload.timestamp, :microsecond)
        row_json = %{"timestamp" => bq_timestamp, "log_message" => payload.log_message}

        row = %GoogleApi.BigQuery.V2.Model.TableDataInsertAllRequestRows{
          insertId: Ecto.UUID.generate(),
          json: row_json
        }
      end)

    BigQuery.stream_batch(String.to_atom(@table_name), rows)

    Logger.info("Bot batch from ETS")
    messages
  end

  defp process_data(data) do
    # Do some calculations, generate a JSON representation, process images.
    IO.inspect(data, labe: "Data processed")
  end
end
