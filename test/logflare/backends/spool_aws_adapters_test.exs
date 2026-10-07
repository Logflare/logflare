defmodule Logflare.Backends.SpoolAwsAdaptersTest do
  use ExUnit.Case, async: true

  import Mimic

  alias Logflare.Backends.Spool.Queue.SQS
  alias Logflare.Backends.Spool.Queue.SQS.HttpClient, as: SQSHttpClient
  alias Logflare.Backends.Spool.Storage.S3
  alias Logflare.Backends.Spool.Storage.S3.HttpClient, as: S3HttpClient

  describe "Storage.S3.put/4" do
    test "sends content-type and content-encoding as real S3 headers, on S3's own dedicated pool" do
      test_pid = self()

      stub(ExAws, :request, fn %ExAws.Operation.S3{} = op, opts ->
        send(test_pid, {:put_called, op.headers, opts[:http_client]})
        {:ok, %{}}
      end)

      assert {:ok, %{}} =
               S3.put("test-bucket", "0/abc.etf.zst", "binary-data",
                 headers: %{
                   "content-type" => "application/x-ndjson",
                   "content-encoding" => "zstd"
                 }
               )

      assert_received {:put_called, headers, http_client}
      assert headers["content-type"] == "application/x-ndjson"
      assert headers["content-encoding"] == "zstd"
      assert http_client == S3HttpClient
    end

    test "defaults content-type to application/octet-stream and omits content-encoding when not provided" do
      test_pid = self()

      stub(ExAws, :request, fn %ExAws.Operation.S3{} = op, _opts ->
        send(test_pid, {:headers_used, op.headers})
        {:ok, %{}}
      end)

      assert {:ok, %{}} = S3.put("test-bucket", "0/abc.etf", "binary-data", [])

      assert_received {:headers_used, headers}
      assert headers["content-type"] == "application/octet-stream"
      refute Map.has_key?(headers, "content-encoding")
    end

    test "returns {:error, reason} on request failure" do
      stub(ExAws, :request, fn _op, _opts -> {:error, "AccessDenied"} end)

      assert {:error, "AccessDenied"} =
               S3.put("test-bucket", "0/abc.etf", "binary-data", headers: %{})
    end
  end

  describe "Storage.S3.get/2" do
    test "returns binary body on success" do
      stub(ExAws, :request, fn _op, _opts ->
        {:ok, %{body: "file-contents"}}
      end)

      assert {:ok, "file-contents"} = S3.get("test-bucket", "0/abc.ndjson.gz")
    end

    test "normalizes a missing object (404) to {:error, :not_found}" do
      stub(ExAws, :request, fn _op, _opts ->
        {:error, {:http_error, 404, "Not Found"}}
      end)

      assert {:error, :not_found} = S3.get("test-bucket", "missing-key")
    end

    test "passes through other errors unchanged" do
      stub(ExAws, :request, fn _op, _opts ->
        {:error, {:http_error, 500, "Internal Server Error"}}
      end)

      assert {:error, {:http_error, 500, "Internal Server Error"}} =
               S3.get("test-bucket", "0/abc.ndjson.gz")
    end
  end

  describe "Queue.SQS.ack/2" do
    test "acknowledges successfully on a normal response" do
      stub(ExAws, :request, fn _op, _opts -> {:ok, %{body: %{}}} end)

      assert :ok = SQS.ack("http://fake/queue", "handle-1")
    end

    test "treats ElasticMQ's empty-body DeleteMessage response as success" do
      # ElasticMQ returns 200 with an empty body for DeleteMessage. Our XML
      # parser (xmerl, via SweetXml) can't parse an empty document and exits
      # with this exact reason even though the delete itself landed.
      stub(ExAws, :request, fn _op, _opts ->
        exit(
          {:fatal,
           {:expected_element_start_tag, {:file, :file_name_unknown}, {:line, 1}, {:col, 1}}}
        )
      end)

      assert :ok = SQS.ack("http://fake/queue", "handle-1")
    end

    test "surfaces a genuine request failure instead of swallowing it" do
      stub(ExAws, :request, fn _op, _opts -> {:error, "AccessDenied"} end)

      assert {:error, "AccessDenied"} = SQS.ack("http://fake/queue", "handle-1")
    end

    test "surfaces an unrelated exit reason as an error instead of assuming success" do
      stub(ExAws, :request, fn _op, _opts -> exit(:some_other_reason) end)

      assert {:error, :some_other_reason} = SQS.ack("http://fake/queue", "handle-1")
    end
  end

  describe "Queue.SQS calls route through SQS's own dedicated pool" do
    test "publish/2 passes http_client: HttpClient to ExAws.request/2" do
      stub(ExAws, :request, fn _op, opts ->
        assert opts[:http_client] == SQSHttpClient
        {:ok, %{}}
      end)

      assert :ok = SQS.publish("http://fake/queue", "body")
    end
  end
end
