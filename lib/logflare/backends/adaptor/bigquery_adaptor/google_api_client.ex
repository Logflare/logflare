defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient do
  @moduledoc false
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData
  alias Google.Cloud.Bigquery.Storage.V1.ArrowRecordBatch
  alias Google.Cloud.Bigquery.Storage.V1.ArrowSchema
  require Logger

  def append_rows({:arrow, record_batch_emf, schema_emf}, project, dataset, table) do
    partition_count = System.schedulers_online()
    partition = :erlang.phash2(self(), partition_count)

    metadata = %{partition: partition}

    {:ok, goth_token} = Goth.fetch({Logflare.Goth, partition})
    # cred = GRPC.Credential.new(ssl: [cacertfile: ca_path])
    {:ok, channel} =
      GRPC.Stub.connect("bigquerystorage.googleapis.com:443",
        # adapter: GRPC.Client.Adapters.Mint,
        cred:
          GRPC.Credential.new(
            ssl: [
              customize_hostname_check: [
                match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
              ]
            ]
          ),
        headers: [
          # {"Content-Type", "application/x-protobuf"},
          {"Authorization", "Bearer #{goth_token.token}"}
        ]
      )

    arrow_rows =
      ArrowData.new(
        writer_schema: ArrowSchema.new(serialized_schema: schema_emf),
        rows: ArrowRecordBatch.new(serialized_record_batch: record_batch_emf)
      )
      |> dbg()

    request =
      AppendRowsRequest.new(
        write_stream: "projects/#{project}/datasets/#{dataset}/tables/#{table}/streams/_default",
        arrow_rows: arrow_rows
      )

    stream =
      channel
      |> BigQueryWrite.Stub.append_rows()

    GRPC.Stub.send_request(stream, request, end_stream: true)

    GRPC.Stub.recv(stream)
    |> case do
      {:ok, responses} = res ->
        Enum.map(responses, fn res -> dbg(res) end)
        res

      {:error, response} = err ->
        Logger.warning("Storage Write API AppendRows  error - #{inspect(response)}")
        err
    end
  end
end
