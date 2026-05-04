defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import Mimic

  alias Google.Cloud.Bigquery.Storage.V1.AppendRowsResponse
  alias Google.Cloud.Bigquery.Storage.V1.BigQueryWrite
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  alias Logflare.Networking.GrpcPool

  defp call_append_rows do
    context = [project_id: "my-project", dataset_id: "my-dataset"]
    GoogleApiClient.append_rows({:arrow, [%{}]}, context, "my-table")
  end

  @moduletag capture_log: true

  setup :verify_on_exit!

  setup do
    test_pid = self()

    sleep_fn = fn ms -> send(test_pid, {:sleep, ms}) end
    Application.put_env(:logflare, GoogleApiClient, sleep: sleep_fn)
    on_exit(fn -> Application.put_env(:logflare, GoogleApiClient, []) end)

    stub(GrpcPool, :get_channel, fn _ -> {:ok, %GRPC.Channel{}} end)
    stub(BigQueryWrite.Stub, :append_rows, fn _ -> %GRPC.Client.Stream{} end)
    stub(GRPC.Stub, :send_request, fn stream, _ -> stream end)
    stub(GRPC.Stub, :end_stream, fn stream -> stream end)
    stub(GRPC.Stub, :recv, fn _ -> {:ok, []} end)

    :ok
  end

  describe "Succseeds on" do
    test "recv empty responses" do
      expect(GRPC.Stub, :recv, fn _ -> {:ok, []} end)
      assert :ok = call_append_rows()
      refute_received {:sleep, _}
    end

    test "clean response with no row errors" do
      expect(GRPC.Stub, :recv, fn _ ->
        {:ok, [{:ok, %AppendRowsResponse{row_errors: []}}]}
      end)

      assert :ok = call_append_rows()
      refute_received {:sleep, _}
    end

    test "response with row_errors" do
      row_error = %{message: "bad row"}

      stub(GRPC.Stub, :recv, fn _ ->
        {:ok, [{:ok, %AppendRowsResponse{row_errors: [row_error]}}]}
      end)

      assert {:ok, log} = with_log(fn -> call_append_rows() end)
      assert log =~ "bad row"
      refute_received {:sleep, _}
    end
  end

  describe "Retry when recv returns" do
    test "RPCError with :unavailable status is returned directly" do
      error = %GRPC.RPCError{status: :unavailable, message: "service unavailable"}
      expect(GRPC.Stub, :recv, fn _ -> {:error, error} end)
      expect(GRPC.Stub, :recv, fn _ -> {:ok, []} end)

      assert :ok = call_append_rows()

      assert_received {:sleep, _}
      refute_received {:sleep, _}
    end

    test "adapter string error" do
      error = "Error occurred while receiving data: :closed"
      expect(GRPC.Stub, :recv, fn _ -> {:error, error} end)
      expect(GRPC.Stub, :recv, fn _ -> {:ok, []} end)

      assert :ok = call_append_rows()

      assert_received {:sleep, _}
      refute_received {:sleep, _}
    end

    test "response with error field" do
      error = {:error, %Google.Rpc.Status{message: "insert error"}}

      expect(GRPC.Stub, :recv, fn _ ->
        {:ok, [{:ok, %AppendRowsResponse{response: error}}]}
      end)

      result = {:append_result, %AppendRowsResponse.AppendResult{}}

      expect(GRPC.Stub, :recv, fn _ ->
        {:ok, [{:ok, %AppendRowsResponse{response: result}}]}
      end)

      assert :ok = call_append_rows()

      assert_received {:sleep, _}
      refute_received {:sleep, _}
    end
  end

  describe "Retry when get_channel returns" do
    test "transient error, then valid channel" do
      test_pid = self()

      expect(GrpcPool, :get_channel, fn _ ->
        send(test_pid, :channel_attempt)
        {:error, :no_channel}
      end)

      expect(GrpcPool, :get_channel, fn _ ->
        send(test_pid, :channel_attempt)
        {:ok, %GRPC.Channel{}}
      end)

      assert :ok = call_append_rows()
      assert_received :channel_attempt
      assert_received {:sleep, _}
      assert_received :channel_attempt
      refute_received {:sleep, _}
    end
  end

  describe "When connection goes down" do
    test "on send_request call" do
      # Simulate Mint adapter call when connection process went down
      expect(GRPC.Stub, :send_request, fn _stream, _req ->
        GenServer.call(Invalid, {:stream_body, make_ref(), "data"})
      end)

      expect(GRPC.Stub, :send_request, fn stream, _req -> stream end)

      assert :ok = call_append_rows()
      assert_received {:sleep, _}
      refute_received {:sleep, _}
    end

    test "on end_stream call" do
      # Simulate Mint adapter call when connection process went down
      expect(GRPC.Stub, :end_stream, fn _ ->
        GenServer.call(Invalid, {:stream_body, make_ref(), :eof})
      end)

      expect(GRPC.Stub, :end_stream, fn stream -> stream end)

      assert :ok = call_append_rows()
      assert_received {:sleep, _}
      refute_received {:sleep, _}
    end
  end

  test "retry exhaustion after 5 retries" do
    error = {:error, :not_connected}

    for _ <- 0..5 do
      expect(GrpcPool, :get_channel, fn _ -> error end)
    end

    assert ^error = call_append_rows()

    assert_received {:sleep, 200}
    assert_received {:sleep, 400}
    assert_received {:sleep, 800}
    assert_received {:sleep, 1600}
    assert_received {:sleep, 2000}
    refute_received {:sleep, _}
  end
end
