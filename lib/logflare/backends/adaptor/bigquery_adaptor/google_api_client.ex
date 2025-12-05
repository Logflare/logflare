defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient do
  @moduledoc false
  alias Google.Rpc.DebugInfo
  alias Google.Cloud.Bigquery.Storage.V1.StorageError
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData
  alias Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC
  require Logger

  @finch_instance_name Logflare.FinchBQStorageWrite

  def append_rows({:arrow, data_frame}, context, table) do
    partition_count = System.schedulers_online()
    partition = :erlang.phash2(self(), partition_count)

    project = context[:project_id]
    dataset = context[:dataset_id]

    {:ok, goth_token} = Goth.fetch({Logflare.Goth, partition})

    {:ok, channel} =
      GRPC.Stub.connect("https://bigquerystorage.googleapis.com",
        adapter: GRPC.Client.Adapters.Finch,
        adapter_opts: [instance_name: @finch_instance_name],
        headers: [
          {"Authorization", "Bearer #{goth_token.token}"}
        ]
      )

    {arrow_schema, batch_msgs} =
      data_frame
      |> Jason.encode!()
      |> ArrowIPC.get_ipc_bytes()

    writer_schema = %Google.Cloud.Bigquery.Storage.V1.ArrowSchema{serialized_schema: arrow_schema}

    stream = BigQueryWrite.Stub.append_rows(channel)

    if length(batch_msgs) > 1 do
      Logger.warning("Storage Write ArrowIPC.get_ipc_bytes produced more than one batch message")
    end

    Enum.each(
      batch_msgs,
      fn ipc_msg ->
        arrow_record_batch = %ArrowRecordBatch{
          serialized_record_batch: ipc_msg
        }

        arrow_rows = %ArrowData{rows: arrow_record_batch, writer_schema: writer_schema}

        request =
          %AppendRowsRequest{
            write_stream:
              "projects/#{project}/datasets/#{dataset}/tables/#{table}/streams/_default",
            rows: {:arrow_rows, arrow_rows}
          }

        GRPC.Stub.send_request(stream, request)
      end
    )

    GRPC.Stub.end_stream(stream)

    GRPC.Stub.recv(stream)
    |> case do
      {:ok, responses} ->
        Enum.each(responses, fn
          {:error, response} ->
            Logger.warning("Storage Write API AppendRows response error - #{inspect(response)}")

          {:ok, %{response: {:error, %{message: msg}}}} ->
            Logger.warning(
              "Storage Write API AppendRows response with error msg - #{inspect(msg)}"
            )

            :ok

          _ ->
            :ok
        end)

        :ok

      {:error, response} = err ->
        Logger.warning("Storage Write API AppendRows  error - #{inspect(response)}")
        err
    end
  end

  def get_finch_instance_name() do
    @finch_instance_name
  end
end
