defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient do
  @moduledoc false
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest
  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsRequest.ArrowData
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
        # cred: cred,
        headers: [
# {"Content-Type", "application/x-protobuf"},
          {"Authorization", "Bearer #{goth_token.token}"}
        ]
      )

    request =
    AppendRowsRequest.new(
        write_stream: "projects/#{project}/datasets/#{dataset}/tables/#{table}/streams/_default",
        arrow_rows: %ArrowData{
          writer_schema: schema_emf,
          rows: record_batch_emf
        }
      )
      |> dbg()

    stream = channel
    |> BigQueryWrite.Stub.append_rows()
|> dbg()

    GRPC.Stub.send_request(stream, request , end_stream: true)
    |> dbg()

    GRPC.Stub.recv(stream)
    |> dbg()
    |> case do
      {:ok, _} = res ->
        res

      {:error, response} = err ->
        Logger.warning("Storage Write API AppendRows  error - #{inspect(response)}")
        err
    end
  end
end
