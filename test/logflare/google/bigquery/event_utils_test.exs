defmodule Logflare.Google.BigQuery.EventUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.EventUtils

  doctest EventUtils

  describe "prepare_for_ingest/1" do
    test "wraps event in list and nested maps in lists" do
      event = %{"message" => "hello", "metadata" => %{"user_id" => "123"}}

      result = EventUtils.prepare_for_ingest(event)
      expected = [%{"message" => "hello", "metadata" => [%{"user_id" => "123"}]}]

      assert result == expected
    end

    test "handles lists of maps unchanged" do
      event = %{"tags" => [%{"key" => "env", "value" => "prod"}]}

      result = EventUtils.prepare_for_ingest(event)

      assert result == [event]
    end
  end
end
