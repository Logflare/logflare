defmodule Logflare.Logs.OtelTraceTest do
  use Logflare.DataCase
  alias Logflare.Logs.OtelTrace

  describe "handle_batch/1" do
    setup do
      user = build(:user)
      source = insert(:source, user: user)
      %{resource_spans: TestUtilsGrpc.random_resource_span(), source: source}
    end

    test "attributes", %{
      resource_spans: resource_spans,
      source: source
    } do
      [%{"attributes" => a} | _] = OtelTrace.handle_batch(resource_spans, source)
      assert a != %{}
      assert Enum.any?(Map.values(a), &is_list/1)
      assert Enum.any?(Map.values(a), &is_number/1)
      assert Enum.any?(Map.values(a), &is_binary/1)
      assert Enum.any?(Map.values(a), &is_boolean/1)
    end

    test "Creates params from a list with one Resource Span that contains an Event", %{
      resource_spans: resource_spans,
      source: source
    } do
      batch = OtelTrace.handle_batch(resource_spans, source)

      span =
        resource_spans
        |> hd()
        |> then(fn rs -> rs.scope_spans end)
        |> hd()
        |> then(fn ss -> ss.spans end)
        |> hd()

      le_span = Enum.find(batch, fn params -> params["metadata"]["type"] == "span" end)

      assert le_span["trace_id"] == Base.encode16(span.trace_id, case: :lower)
      assert le_span["span_id"] == Base.encode16(span.span_id, case: :lower)
      assert le_span["parent_span_id"] == Base.encode16(span.parent_span_id, case: :lower)
      assert le_span["event_message"] == span.name

      assert elem(DateTime.from_iso8601(le_span["timestamp"]), 1) ==
               DateTime.from_unix!(span.start_time_unix_nano, :nanosecond)

      assert elem(DateTime.from_iso8601(le_span["start_time"]), 1) ==
               elem(DateTime.from_unix(span.start_time_unix_nano, :nanosecond), 1)

      assert elem(DateTime.from_iso8601(le_span["end_time"]), 1) ==
               elem(DateTime.from_unix(span.end_time_unix_nano, :nanosecond), 1)

      event = hd(span.events)
      le_event = Enum.find(batch, fn params -> params["metadata"]["type"] == "event" end)

      assert le_event["event_message"] == event.name
      assert le_event["parent_span_id"] == Base.encode16(span.span_id)

      assert elem(DateTime.from_iso8601(le_event["timestamp"]), 1) ==
               DateTime.from_unix!(event.time_unix_nano, :nanosecond)
    end

    test "json parsable log event body", %{
      resource_spans: resource_spans,
      source: source
    } do
      [params | _] = resource_spans |> OtelTrace.handle_batch(source)
      assert {:ok, _} = Jason.encode(params)
    end
  end
end
