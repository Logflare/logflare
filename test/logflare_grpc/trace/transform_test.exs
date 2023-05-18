defmodule LogflareGrpc.Trace.TransformTest do
  use Logflare.DataCase
  alias LogflareGrpc.Trace.Transform

  describe "to_log_events/1" do
    setup do
      user = build(:user)
      source = insert(:source, user: user)
      %{resource_spans: TestUtilsGrpc.random_resource_span(), source: source}
    end

    test "Creates LogEvent from a list with one Resource Span that contains an Event", %{
      resource_spans: resource_spans,
      source: source
    } do
      log_events = Transform.to_log_events(resource_spans, source)
      assert Enum.all?(log_events, fn %{source: %{id: id}} -> source.id == id end)

      span =
        resource_spans
        |> hd()
        |> then(fn rs -> rs.scope_spans end)
        |> hd()
        |> then(fn ss -> ss.spans end)
        |> hd()

      le_span = Enum.find(log_events, fn le -> le.body["metadata"]["type"] == "span" end)

      assert le_span.body["trace_id"] == elem(Ecto.UUID.cast(span.trace_id), 1)
      assert le_span.body["span_id"] == Base.encode16(span.span_id)
      assert le_span.body["parent_span_id"] == Base.encode16(span.parent_span_id)
      assert le_span.body["event_message"] == span.name

      assert DateTime.from_unix(le_span.body["timestamp"], :microsecond) ==
               DateTime.from_unix(span.start_time_unix_nano, :nanosecond)

      assert elem(DateTime.from_iso8601(le_span.body["start_time"]), 1) ==
               elem(DateTime.from_unix(span.start_time_unix_nano, :nanosecond), 1)

      assert elem(DateTime.from_iso8601(le_span.body["end_time"]), 1) ==
               elem(DateTime.from_unix(span.end_time_unix_nano, :nanosecond), 1)

      event = hd(span.events)
      le_event = Enum.find(log_events, fn le -> le.body["metadata"]["type"] == "event" end)

      assert le_event.body["event_message"] == event.name
      assert le_event.body["parent_span_id"] == Base.encode16(span.span_id)

      assert DateTime.from_unix(le_event.body["timestamp"], :microsecond) ==
               DateTime.from_unix(event.time_unix_nano, :nanosecond)
    end

    test "json parsable log event body", %{
      resource_spans: resource_spans,
      source: source
    } do
      log_events_body = resource_spans |> Transform.to_log_events(source) |> Enum.map(& &1.body)
      assert {:ok, _} = Jason.encode(log_events_body)
    end
  end
end
