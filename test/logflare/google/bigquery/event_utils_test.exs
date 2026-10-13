defmodule Logflare.Google.BigQuery.EventUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.EventUtils

  doctest EventUtils

  describe "log_event_to_df_struct/1" do
    @base_body %{
      "timestamp" => 1_779_436_901_362_775,
      "event_message" => "test"
    }

    test "converts start_time from nanoseconds to int64 unix microseconds when otel_timestamps is set" do
      le = %Logflare.LogEvent{
        body: Map.put(@base_body, "start_time", 1_779_436_330_890_427_000),
        otel_timestamps: true
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert result["start_time"] == 1_779_436_330_890_427
    end

    test "converts both start_time and end_time from nanoseconds to int64 unix microseconds" do
      le = %Logflare.LogEvent{
        body:
          @base_body
          |> Map.put("start_time", 1_779_436_330_890_427_000)
          |> Map.put("end_time", 1_779_436_901_362_775_000),
        otel_timestamps: true
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert result["start_time"] == 1_779_436_330_890_427
      assert result["end_time"] == 1_779_436_901_362_775
    end

    test "converts only end_time from nanoseconds to int64 unix microseconds" do
      le = %Logflare.LogEvent{
        body: Map.put(@base_body, "end_time", 1_779_436_901_362_775_000),
        otel_timestamps: true
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert result["end_time"] == 1_779_436_901_362_775
    end

    test "leaves start_time unchanged when otel_timestamps is not set" do
      le = %Logflare.LogEvent{
        body: Map.put(@base_body, "start_time", 1_779_436_330_890_427_000)
      }

      result = EventUtils.log_event_to_df_struct(le)

      assert result["start_time"] == 1_779_436_330_890_427_000
    end

    test "leaves timestamp unchanged as int64 unix microseconds" do
      le = %Logflare.LogEvent{body: @base_body}

      result = EventUtils.log_event_to_df_struct(le)

      assert result["timestamp"] == 1_779_436_901_362_775
    end
  end

  describe "convert_to_microseconds/2" do
    @ns 1_779_436_330_890_427_000
    @us 1_779_436_901_362_775

    test "converts nanosecond start_time and end_time to int64 unix microseconds for OTel events" do
      body = %{"start_time" => @ns, "end_time" => 1_779_436_901_362_775_000}

      result = EventUtils.convert_to_microseconds(body, true)

      assert result["start_time"] == 1_779_436_330_890_427
      assert result["end_time"] == 1_779_436_901_362_775
    end

    test "does not touch timestamp, which is already unix microseconds" do
      assert EventUtils.convert_to_microseconds(%{"timestamp" => @us}, false) ==
               %{"timestamp" => @us}
    end

    test "leaves start_time unchanged when not nanoseconds" do
      body = %{"start_time" => 1_234_567_890}

      assert EventUtils.convert_to_microseconds(body, true) == body
    end

    test "leaves non-OTel start_time and end_time unchanged even in the nanosecond range" do
      body = %{"start_time" => @ns, "end_time" => 1_779_436_901_362_775_000}

      assert EventUtils.convert_to_microseconds(body, false) == body
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

    test "handles nested list-of-lists (e.g. a serialized stacktrace)" do
      event = %{
        "stacktrace" => [
          ["Elixir.ProjectThree.TmAmTicket", "ingest", 2],
          ["Elixir.ProjectThree.TmHost", "dispatch", 1]
        ]
      }

      assert EventUtils.prepare_for_ingest(event) == [event]
    end

    test "handles a list whose head is a map but tail contains lists and scalars" do
      event = %{
        "mixed" => [
          %{"a" => %{"b" => 1}},
          ["nested", "list"],
          "scalar"
        ]
      }

      result = EventUtils.prepare_for_ingest(event)

      expected = [
        %{
          "mixed" => [
            %{"a" => [%{"b" => 1}]},
            ["nested", "list"],
            "scalar"
          ]
        }
      ]

      assert result == expected
    end
  end
end
