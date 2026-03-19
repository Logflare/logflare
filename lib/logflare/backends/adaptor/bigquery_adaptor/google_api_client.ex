defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient do
  @moduledoc false

  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsResponse
  alias Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC
  alias Logflare.Networking.GrpcPool

  require Logger
  require OpenTelemetry.Tracer

  @finch_instance_name Logflare.FinchBQStorageWrite

  @spec connetion_pool_name() :: module()
  def connetion_pool_name(), do: GrpcPool

  @spec encode_ndjson([map()]) :: binary()
  def encode_ndjson(data_frames) do
    OpenTelemetry.Tracer.with_span :bq_ndjson_encode do
      ndjson = Enum.map_join(data_frames, "\n", &Jason.encode!/1)
      OpenTelemetry.Tracer.set_attribute(:ndjson_bytes, byte_size(ndjson))
      ndjson
    end
  end

  def encode_arrow_data(ndjson) do
    {arrow_schema, batch_msgs} =
      OpenTelemetry.Tracer.with_span :bq_ipc_encode do
        {_schema, msgs} = r = ArrowIPC.get_ipc_bytes(ndjson)
        OpenTelemetry.Tracer.set_attribute(:ipc_batch_count, length(msgs))
        r
      end

    if length(batch_msgs) > 1 do
      Logger.warning("Storage Write ArrowIPC.get_ipc_bytes produced more than one batch message")
    end

    writer_schema = %Google.Cloud.Bigquery.Storage.V1.ArrowSchema{serialized_schema: arrow_schema}

    for ipc_msg <- batch_msgs do
      arrow_record_batch = %ArrowRecordBatch{
        serialized_record_batch: ipc_msg
      }

      %ArrowData{rows: arrow_record_batch, writer_schema: writer_schema}
    end
  end

  def append_rows({:arrow, arrow_rows}, context, table, impl \\ :finch) do
    project = context[:project_id]
    dataset = context[:dataset_id]

    with {:ok, channel} <- get_channel(impl) do
      stream = BigQueryWrite.Stub.append_rows(channel)

      stream =
        Enum.reduce(
          arrow_rows,
          stream,
          fn arrow_data, stream ->
            request =
              %AppendRowsRequest{
                write_stream:
                  "projects/#{project}/datasets/#{dataset}/tables/#{table}/streams/_default",
                rows: {:arrow_rows, arrow_data}
              }

            GRPC.Stub.send_request(stream, request)
          end
        )

      GRPC.Stub.end_stream(stream)

      GRPC.Stub.recv(stream)
      |> case do
        {:ok, responses} ->
          insert_error_count =
            Enum.reduce(responses, 0, fn
              {:error, response}, acc ->
                Logger.warning(
                  "Storage Write API AppendRows response error - #{inspect(response)}"
                )

                acc

              {:ok, %AppendRowsResponse{response: {:error, %{message: msg}}}}, acc ->
                Logger.warning(
                  "Storage Write API AppendRows response with error msg - #{inspect(msg)}"
                )

                acc

              {:ok, %AppendRowsResponse{row_errors: []}}, acc ->
                acc

              {:ok, %AppendRowsResponse{row_errors: errors}}, acc when is_list(errors) ->
                Logger.warning("Storage Write API AppendRows row errors - #{inspect(errors)}")
                length(errors) + acc
            end)

          OpenTelemetry.Tracer.set_attribute(:insert_error_count, insert_error_count)

          :ok

        {:error, response} = err ->
          Logger.warning("Storage Write API AppendRows error - #{inspect(response)}")
          err
      end
    end
  end

  defp get_channel(:finch) do
    partition_count = System.schedulers_online()
    partition = :erlang.phash2(self(), partition_count)

    with {:ok, goth_token} <- Goth.fetch({Logflare.Goth, partition}) do
      GRPC.Stub.connect("https://bigquerystorage.googleapis.com",
        adapter: GRPC.Client.Adapters.Finch,
        adapter_opts: [instance_name: @finch_instance_name],
        headers: [{"Authorization", "Bearer #{goth_token.token}"}]
      )
    end
  end

  defp get_channel(:mint) do
    GrpcPool.get_channel(connetion_pool_name())
  end

  def get_finch_instance_name, do: @finch_instance_name
end
