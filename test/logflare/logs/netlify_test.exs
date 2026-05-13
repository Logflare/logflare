defmodule Logflare.Logs.NetlifyTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Logflare.Logs.Netlify

  describe "handle_batch/2" do
    test "processes an empty batch" do
      assert Netlify.handle_batch([], %{}) == []
    end
  end

  describe "handle_batch/2 with traffic log_type" do
    test "formats event with url-style fields" do
      event = %{
        "timestamp" => 1_577_836_800_000,
        "log_type" => "traffic",
        "method" => "GET",
        "status_code" => 200,
        "client_ip" => "192.168.1.1",
        "request_id" => "abc-123",
        "url" => "/page",
        "user_agent" => "Mozilla/5.0"
      }

      [result] = Netlify.handle_batch([event], %{})

      assert result["timestamp"] == 1_577_836_800_000

      assert result["message"] ==
               "traffic | GET | 200 | 192.168.1.1 | abc-123 | /page | Mozilla/5.0"

      assert is_map(result["metadata"])
      refute Map.has_key?(result["metadata"], "timestamp")
      assert result["metadata"]["log_type"] == "traffic"
    end

    test "preserves extra fields in metadata" do
      event = %{
        "timestamp" => 1_577_836_800_000,
        "log_type" => "traffic",
        "method" => "GET",
        "status_code" => 200,
        "client_ip" => "192.168.1.1",
        "request_id" => "req-1",
        "url" => "/",
        "user_agent" => "Mozilla/5.0",
        "country" => "US",
        "deploy_id" => "abc123"
      }

      [result] = Netlify.handle_batch([event], %{})

      refute Map.has_key?(result["metadata"], "timestamp")
      assert result["metadata"]["country"] == "US"
      assert result["metadata"]["deploy_id"] == "abc123"
    end
  end

  describe "handle_batch/2 with functions log_type" do
    test "formats event with path-style fields" do
      event = %{
        "timestamp" => 1_577_836_800_000,
        "log_type" => "functions",
        "method" => "POST",
        "status_code" => 200,
        "request_id" => "fn-req-1",
        "path" => "/.netlify/functions/hello"
      }

      [result] = Netlify.handle_batch([event], %{})

      assert result["timestamp"] == 1_577_836_800_000

      assert result["message"] ==
               "functions | POST | 200 | fn-req-1 | /.netlify/functions/hello"

      assert is_map(result["metadata"])
      refute Map.has_key?(result["metadata"], "timestamp")
      assert result["metadata"]["log_type"] == "functions"
    end

    test "preserves extra fields in metadata" do
      event = %{
        "timestamp" => 1_577_836_800_000,
        "log_type" => "functions",
        "method" => "GET",
        "status_code" => 200,
        "request_id" => "req-1",
        "path" => "/.netlify/functions/hello",
        "function_name" => "hello",
        "function_type" => "regular"
      }

      [result] = Netlify.handle_batch([event], %{})

      refute Map.has_key?(result["metadata"], "timestamp")
      assert result["metadata"]["function_name"] == "hello"
      assert result["metadata"]["function_type"] == "regular"
    end
  end

  describe "handle_batch/2 with message-only payload" do
    test "tags as validate and preserves original message" do
      event = %{"message" => "deploy validation started"}

      [result] = Netlify.handle_batch([event], %{})

      assert result == %{
               "message" => "deploy validation started",
               "metadata" => %{"log_type" => "validate"}
             }
    end
  end

  describe "custom_message/1 fallback" do
    test "falls back to JSON encoding when fields don't match any pattern" do
      event = %{
        "timestamp" => 1_577_836_800_000,
        "log_type" => "traffic",
        "unknown_field" => "value"
      }

      log =
        capture_log(fn ->
          [result] = Netlify.handle_batch([event], %{})

          assert is_binary(result["message"])
          assert result["metadata"]["log_type"] == "traffic"
        end)

      assert log =~ "Unhandled Netlify log event!"
    end

    test "returns error message when JSON encoding fails" do
      event = %{
        "timestamp" => 1_577_836_800_000,
        "log_type" => "traffic",
        "bad_value" => self()
      }

      log =
        capture_log(fn ->
          [result] = Netlify.handle_batch([event], %{})

          assert result["message"] == "Error in decoding unhandled Netlify log event format!"
        end)

      assert log =~ "Error in decoding unhandled Netlify log event format!"
    end
  end
end
