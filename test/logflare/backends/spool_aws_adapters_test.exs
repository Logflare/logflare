defmodule Logflare.Backends.SpoolAwsAdaptersTest do
  use ExUnit.Case, async: true

  import Mimic

  alias Logflare.Backends.Spool.Queue.SQS
  alias Logflare.Backends.Spool.Storage.S3

  describe "Storage.S3.get/2" do
    test "returns binary body on success" do
      stub(ExAws, :request, fn _op ->
        {:ok, %{body: "file-contents"}}
      end)

      assert {:ok, "file-contents"} = S3.get("test-bucket", "0/abc.ndjson.gz")
    end

    test "normalizes a missing object (404) to {:error, :not_found}" do
      stub(ExAws, :request, fn _op ->
        {:error, {:http_error, 404, "Not Found"}}
      end)

      assert {:error, :not_found} = S3.get("test-bucket", "missing-key")
    end

    test "passes through other errors unchanged" do
      stub(ExAws, :request, fn _op ->
        {:error, {:http_error, 500, "Internal Server Error"}}
      end)

      assert {:error, {:http_error, 500, "Internal Server Error"}} =
               S3.get("test-bucket", "0/abc.ndjson.gz")
    end
  end

  describe "Queue.SQS.ack/2" do
    test "acknowledges successfully on a normal response" do
      stub(ExAws, :request, fn _op -> {:ok, %{body: %{}}} end)

      assert :ok = SQS.ack("http://fake/queue", "handle-1")
    end

    test "treats ElasticMQ's empty-body DeleteMessage response as success" do
      # ElasticMQ returns 200 with an empty body for DeleteMessage. Our XML
      # parser (xmerl, via SweetXml) can't parse an empty document and exits
      # with this exact reason even though the delete itself landed.
      stub(ExAws, :request, fn _op ->
        exit(
          {:fatal,
           {:expected_element_start_tag, {:file, :file_name_unknown}, {:line, 1}, {:col, 1}}}
        )
      end)

      assert :ok = SQS.ack("http://fake/queue", "handle-1")
    end

    test "surfaces a genuine request failure instead of swallowing it" do
      stub(ExAws, :request, fn _op -> {:error, "AccessDenied"} end)

      assert {:error, "AccessDenied"} = SQS.ack("http://fake/queue", "handle-1")
    end

    test "surfaces an unrelated exit reason as an error instead of assuming success" do
      stub(ExAws, :request, fn _op -> exit(:some_other_reason) end)

      assert {:error, :some_other_reason} = SQS.ack("http://fake/queue", "handle-1")
    end
  end
end
