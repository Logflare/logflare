defmodule Logflare.Utils.LoggerMetadataTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  require Logger
  alias Logflare.Utils.LoggerMetadata

  describe "with_metadata/2" do
    setup do
      Logger.metadata(request_id: "before")
    end

    test "sets metadata while the function runs and restores previous metadata" do
      log =
        capture_log([level: :error, metadata: [:request_id, :user_id]], fn ->
          LoggerMetadata.with_metadata([user_id: 123], fn ->
            Logger.error("test")
          end)
        end)

      assert log =~ "request_id=before"
      assert log =~ "user_id=123"
      assert Logger.metadata() == [request_id: "before"]
    end

    test "restores previous metadata when the function raises" do
      assert_raise RuntimeError, "boom", fn ->
        LoggerMetadata.with_metadata([user_id: 123], fn ->
          raise "boom"
        end)
      end

      assert Logger.metadata() == [request_id: "before"]
    end
  end
end
