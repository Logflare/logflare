defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPCTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC

  describe "get_ipc_bytes/2" do
    test "forces start_time/end_time to Arrow Timestamp when is_otel is true" do
      json =
        Jason.encode!(%{
          "timestamp" => 1_779_436_901_362_775,
          "start_time" => 1_779_436_330_890_427,
          "end_time" => 1_779_436_901_362_775
        })

      {otel_schema, _batches} = ArrowIPC.get_ipc_bytes(json, true)
      {plain_schema, _batches} = ArrowIPC.get_ipc_bytes(json, false)

      refute otel_schema == plain_schema
    end

    test "does not force start_time/end_time to Arrow Timestamp when is_otel is false" do
      # a non-OTel event that happens to use "start_time"/"end_time" as regular field
      # names should be unaffected by the OTel schema override
      json = Jason.encode!(%{"timestamp" => 1_779_436_901_362_775, "start_time" => 42})

      {schema_no_otel, _} = ArrowIPC.get_ipc_bytes(json, false)
      {schema_default, _} = ArrowIPC.get_ipc_bytes(json)

      assert schema_no_otel == schema_default
    end

    test "is_otel defaults to false" do
      json = Jason.encode!(%{"timestamp" => 1_779_436_901_362_775, "start_time" => 42})

      {schema_default, _} = ArrowIPC.get_ipc_bytes(json)
      {schema_explicit_false, _} = ArrowIPC.get_ipc_bytes(json, false)

      assert schema_default == schema_explicit_false
    end
  end
end
