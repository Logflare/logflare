defmodule Logflare.Google.BigQuery.EventUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.EventUtils

  doctest EventUtils

  describe "log_event_to_df_struct/1" do
    @base_body %{
      "timestamp" => 1_779_436_901_362_775,
      "event_message" => "test"
    }

    test "converts start_time from nanoseconds to seconds (float)" do
      le = %Logflare.LogEvent{
        body: Map.put(@base_body, "start_time", 1_779_436_330_890_427_000)
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert_in_delta result["start_time"], 1_779_436_330.890_427, 1.0e-6
    end

    test "converts both start_time and end_time from nanoseconds to seconds (float)" do
      le = %Logflare.LogEvent{
        body:
          @base_body
          |> Map.put("start_time", 1_779_436_330_890_427_000)
          |> Map.put("end_time", 1_779_436_901_362_775_000)
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert_in_delta result["start_time"], 1_779_436_330.890_427, 1.0e-6
      assert result["end_time"] == 1_779_436_901.362_775
    end

    test "converts only end_time from nanoseconds to seconds (float)" do
      le = %Logflare.LogEvent{
        body: Map.put(@base_body, "end_time", 1_779_436_901_362_775_000)
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert result["end_time"] == 1_779_436_901.362_775
    end

    test "converts start_time from nanoseconds to seconds (float) regardless of event type" do
      le = %Logflare.LogEvent{
        body: Map.put(@base_body, "start_time", 1_779_436_330_890_427_000)
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert_in_delta result["start_time"], 1_779_436_330.890_427, 1.0e-6
    end

    test "passes timestamp through as seconds (float)" do
      le = %Logflare.LogEvent{body: @base_body}

      result = EventUtils.log_event_to_df_struct(le)

      assert result["timestamp"] == 1_779_436_901.362_775
    end
  end

  describe "convert_to_seconds/1" do
    @ns 1_779_436_330_890_427_000
    @us 1_779_436_901_362_775

    test "converts nanosecond start_time and end_time to float seconds" do
      body = %{"start_time" => @ns, "end_time" => 1_779_436_901_362_775_000}

      result = EventUtils.convert_to_seconds(body)

      assert result["start_time"] == 1_779_436_330.8904269
      assert result["end_time"] == 1_779_436_901.362_775
    end

    test "converts microsecond timestamp to float seconds" do
      body = %{"timestamp" => @us}

      result = EventUtils.convert_to_seconds(body)

      assert result["timestamp"] == 1_779_436_901.362_775
    end

    test "leaves start_time unchanged when not nanoseconds" do
      body = %{"start_time" => 1_234_567_890}

      assert EventUtils.convert_to_seconds(body) == body
    end
  end

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
