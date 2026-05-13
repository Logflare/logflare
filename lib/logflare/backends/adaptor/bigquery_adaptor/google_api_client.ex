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

  @spec connetion_pool_name :: module()
  def connetion_pool_name, do: GrpcPool

  @spec encode_ndjson([map()]) :: binary()
  def encode_ndjson(data_frames) do
    OpenTelemetry.Tracer.with_span "ingest.bq_ndjson_encode" do
      ndjson = Enum.map_join(data_frames, "\n", &Jason.encode!/1)
      OpenTelemetry.Tracer.set_attribute(:ndjson_bytes, byte_size(ndjson))
      ndjson
    end
  end

  def encode_arrow_data(ndjson) do
    {arrow_schema, batch_msgs} =
      OpenTelemetry.Tracer.with_span "ingest.bq_ipc_encode" do
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

  def append_rows({:arrow, arrow_rows}, context, table) do
    project = context[:project_id]
    dataset = context[:dataset_id]

    requests =
      Enum.map(arrow_rows, fn rows_batch ->
        %AppendRowsRequest{
          write_stream:
            "projects/#{project}/datasets/#{dataset}/tables/#{table}/streams/_default",
          rows: {:arrow_rows, rows_batch},
          default_missing_value_interpretation: :NULL_VALUE
        }
      end)

    try_call(requests)
  end

  defp try_call(requests, retry_attempt \\ 0) do
    OpenTelemetry.Tracer.with_span "ingest.call_attempt", %{
      attributes: %{retry_attempt: retry_attempt}
    } do
      with {:ok, channel} <- GrpcPool.get_channel(connetion_pool_name()),
           {:ok, stream} <- send_requests(requests, channel),
           {:ok, responses} <- GRPC.Stub.recv(stream),
           [] <- handle_responses(requests, responses) do
        :ok
      else
        err ->
          OpenTelemetry.Tracer.set_status(:error, inspect(err))
          err
      end
    end
    |> case do
      :ok ->
        :ok

      failed_reqs when is_list(failed_reqs) and retry_attempt < 5 ->
        retry_call(failed_reqs, retry_attempt)

      _err when retry_attempt < 5 ->
        retry_call(requests, retry_attempt)

      err ->
        err
    end
  end

  defp send_requests(requests, channel) do
    stream = BigQueryWrite.Stub.append_rows(channel)

    stream =
      Enum.reduce(
        requests,
        stream,
        fn request, stream ->
          GRPC.Stub.send_request(stream, request)
        end
      )

    {:ok, GRPC.Stub.end_stream(stream)}
  catch
    :exit, reason -> {:error, Exception.format_exit(reason)}
  end

  defp retry_call(requests, retry_attempt) do
    # 200, 400, 800, 1600, 2000, ...
    retry_wait = min(2000, 200 * 2 ** retry_attempt)
    sleep(retry_wait)
    try_call(requests, retry_attempt + 1)
  end

  defp handle_responses(requests, responses) do
    requests
    |> Stream.zip(responses)
    |> Enum.reduce([], fn
      {request, {:error, response}}, acc ->
        Logger.error("Storage Write API AppendRows response error - #{inspect(response)}")

        [request | acc]

      {request, {:ok, %AppendRowsResponse{response: {:error, %{message: msg}}}}}, acc ->
        Logger.warning("Storage Write API AppendRows response with error msg - #{inspect(msg)}")

        [request | acc]

      {_request, {:ok, %AppendRowsResponse{row_errors: []}}}, acc ->
        acc

      {_request, {:ok, %AppendRowsResponse{row_errors: errors}}}, acc when is_list(errors) ->
        Logger.warning("Storage Write API AppendRows row errors - #{inspect(errors)}")
        OpenTelemetry.Tracer.set_attribute("row_errors_present", true)
        # TODO: This will override previous, but most of the time there's only one request
        OpenTelemetry.Tracer.set_attribute("row_errors_count", length(errors))
        acc
    end)
  end

  defp sleep(ms) do
    Application.get_env(:logflare, __MODULE__, [])
    |> Keyword.get(:sleep, &Process.sleep/1)
    |> then(& &1.(ms))
  end
end
