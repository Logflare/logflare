defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient do
  @moduledoc false
  alias Explorer.DataFrame
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData
  alias Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch
  require Logger

  @finch_instance_name Logflare.FinchBQStorageWrite

  def append_rows({:arrow, data_frame}, project, dataset, table) do
    partition_count = System.schedulers_online()
    partition = :erlang.phash2(self(), partition_count)

    {:ok, goth_token} = Goth.fetch({Logflare.Goth, partition})

    {:ok, channel} =
      GRPC.Stub.connect("https://bigquerystorage.googleapis.com",
        adapter: GRPC.Client.Adapters.Finch,
        adapter_opts: [instance_name: @finch_instance_name],
        headers: [
          {"Authorization", "Bearer #{goth_token.token}"}
        ]
      )

    {:ok, arrow_schema} = DataFrame.dump_ipc_schema(data_frame)

    writer_schema = %Google.Cloud.Bigquery.Storage.V1.ArrowSchema{serialized_schema: arrow_schema}

    {:ok, batch_msgs} = DataFrame.dump_ipc_record_batch(data_frame)

    stream = BigQueryWrite.Stub.append_rows(channel)

    task =
      Task.async_stream(
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
        end,
        ordered: false,
        max_concurrency: System.schedulers_online()
      )

    Stream.run(task)

    GRPC.Stub.end_stream(stream)

    GRPC.Stub.recv(stream)
    |> case do
      {:ok, responses} ->
        Enum.each(responses, fn
          {:error, response} ->
            Logger.warning("Storage Write API AppendRows response error - #{inspect(response)}")

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
