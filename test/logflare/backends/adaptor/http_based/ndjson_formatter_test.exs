defmodule Logflare.Backends.Adaptor.HttpBased.NdjsonFormatterTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Logflare.Backends.Adaptor.HttpBased.NdjsonFormatter
  alias Logflare.LogEvent

  test "encodes one event body per line" do
    events = [%LogEvent{body: %{"a" => 1}}, %LogEvent{body: %{"b" => 2}}]

    assert IO.iodata_to_binary(NdjsonFormatter.encode(events)) == ~s({"a":1}\n{"b":2})
  end

  test "passes non-event terms through unchanged" do
    assert NdjsonFormatter.encode("raw") == "raw"
    assert NdjsonFormatter.encode(nil) == nil
  end

  test "drops unencodable events and logs the dropped count" do
    events = [
      %LogEvent{body: %{"ok" => true}},
      %LogEvent{body: %{"bad" => <<0xFF>>}},
      %LogEvent{body: %{"bad2" => <<0xFE>>}}
    ]

    log =
      capture_log(fn ->
        assert IO.iodata_to_binary(NdjsonFormatter.encode(events)) == ~s({"ok":true})
      end)

    assert log =~ "Dropped 2 log events from an NDJSON batch"
  end

  test "attaches the given metadata to the dropped count log" do
    events = [%LogEvent{body: %{"bad" => <<0xFF>>}}]

    log =
      capture_log([format: "$metadata$message", metadata: [:backend_id]], fn ->
        NdjsonFormatter.encode(events, backend_id: 123)
      end)

    assert log =~ "backend_id=123"
  end

  test "returns an error instead of an empty body when every event is dropped" do
    events = [%LogEvent{body: %{"bad" => <<0xFF>>}}, %LogEvent{body: %{"bad2" => <<0xFE>>}}]
    env = %Tesla.Env{body: events, headers: []}

    capture_log(fn ->
      assert NdjsonFormatter.call(env, [], []) == {:error, :all_events_dropped}
    end)
  end

  test "sends an empty batch through unchanged" do
    env = %Tesla.Env{body: [], headers: []}

    assert {:ok, %Tesla.Env{body: []}} = NdjsonFormatter.call(env, [], [])
  end
end
