defmodule Logflare.Backends.Spool.ConsumerPipelineTest do
  use Logflare.DataCase, async: false

  import Mimic
  import Logflare.Factory

  alias Broadway.Message
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.TestUtils

  setup :set_mimic_global

  defp record(source_id, event_id, extra_body \\ %{}) do
    body =
      Map.merge(%{"id" => event_id, "timestamp" => System.system_time(:microsecond)}, extra_body)

    %{"source_id" => source_id, "body" => body, "id" => event_id, "event_type" => "log"}
  end

  # Build a Broadway.Message as it looks *after* handle_message/3 has parsed
  # its segment: data is the list of records the segment decoded into (one
  # segment is one original ingest request's chunk, so usually more than one),
  # acknowledger is the pipeline no-op.
  defp segment_message(records) when is_list(records) do
    %Message{data: records, acknowledger: {ConsumerPipeline, :noop, nil}}
  end

  defp line_message(source_id, event_id, extra_body \\ %{}) do
    segment_message([record(source_id, event_id, extra_body)])
  end

  defp ndjson_segment(records) do
    records
    |> Enum.map(&Jason.encode!/1)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp unparsed_message(segment, format) do
    %Message{
      data: %{segment: segment, format: format},
      acknowledger: {ConsumerPipeline, :noop, %{in_flight_ref: nil, bytes: byte_size(segment)}}
    }
  end

  describe "transform/2" do
    test "wraps the producer's raw segment and stashes its byte size for the in-flight counter" do
      segment = ndjson_segment([%{"id" => "e1"}, %{"id" => "e2"}])
      unparsed = %{segment: segment, format: :ndjson}

      assert %Message{data: ^unparsed, acknowledger: {ConsumerPipeline, :noop, ack_data}} =
               ConsumerPipeline.transform(unparsed, [])

      # max_in_flight is a byte budget, so the acknowledger carries the bytes
      # to give back rather than a message count.
      assert ack_data.bytes == byte_size(segment)
      assert Map.has_key?(ack_data, :in_flight_ref)
    end
  end

  describe "handle_message/3" do
    test "parses an ndjson segment into its records" do
      records = [%{"id" => "e1", "source_id" => 1}, %{"id" => "e2", "source_id" => 1}]
      message = unparsed_message(ndjson_segment(records), :ndjson)

      assert %Message{status: :ok, data: parsed} =
               ConsumerPipeline.handle_message(:default, message, %{})

      assert parsed == records
    end

    test "parses an etf segment into its records" do
      records = [%{"id" => "e1", "source_id" => 1}, %{"id" => "e2", "source_id" => 2}]
      message = unparsed_message(:erlang.term_to_binary(records), :etf)

      assert %Message{status: :ok, data: ^records} =
               ConsumerPipeline.handle_message(:default, message, %{})
    end

    test "skips unparseable lines within an otherwise-valid ndjson segment" do
      segment = ~s({"id":"e1"}\nnot json at all\n{"id":"e2"}\n)
      message = unparsed_message(segment, :ndjson)

      assert %Message{status: :ok, data: parsed} =
               ConsumerPipeline.handle_message(:default, message, %{})

      assert Enum.map(parsed, & &1["id"]) == ["e1", "e2"]
    end

    test "fails only this message when a segment's content cannot be parsed at all" do
      # Well-formed bytes that passed framing/CRC upstream, but not a valid
      # Erlang external term — exactly the ArgumentError :erlang.binary_to_term/1
      # raises on corrupt or format-mismatched spool content. Broadway routes a
      # failed message straight to ack/3 without it ever reaching handle_batch/4.
      message = unparsed_message("this is not valid etf", :etf)

      assert %Message{status: {:failed, _reason}} =
               ConsumerPipeline.handle_message(:default, message, %{})
    end

    test "emits parse telemetry with the segment's event count" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :parse])

      records = [%{"id" => "e1"}, %{"id" => "e2"}, %{"id" => "e3"}]
      message = unparsed_message(ndjson_segment(records), :ndjson)

      ConsumerPipeline.handle_message(:default, message, %{})

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :consumer, :parse],
                      %{segment_count: 1, event_count: 3, duration: _}, %{}}
    end

    test "registers each parsed record's source with MemoryMonitor" do
      test_pid = self()
      stub(MemoryMonitor, :register_source, fn sid -> send(test_pid, {:registered, sid}) end)

      records = [
        %{"id" => "e1", "source_id" => 1},
        %{"id" => "e2", "source_id" => 2}
      ]

      message = unparsed_message(ndjson_segment(records), :ndjson)

      ConsumerPipeline.handle_message(:default, message, %{})

      assert_receive {:registered, 1}
      assert_receive {:registered, 2}
    end

    test "does not register anything for records with no source_id" do
      test_pid = self()
      stub(MemoryMonitor, :register_source, fn sid -> send(test_pid, {:registered, sid}) end)

      message = unparsed_message(ndjson_segment([%{"id" => "e1"}]), :ndjson)

      ConsumerPipeline.handle_message(:default, message, %{})

      refute_receive {:registered, _}
    end
  end

  describe "handle_batch/4" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)
      [source: source]
    end

    test "dispatches events to the correct source by integer source_id", %{source: source} do
      event_id = Ecto.UUID.generate()
      messages = [line_message(source.id, event_id, %{"message" => "hello"})]

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, dispatched_source ->
        send(pid, {:dispatched, event_params, dispatched_source.id})
        {:ok, length(event_params)}
      end)

      ConsumerPipeline.handle_batch(:default, messages, %{}, %{})

      assert_receive {:dispatched, [params], source_id}
      assert source_id == source.id
      assert params["id"] == event_id
    end

    test "dispatches every record of a multi-record message, not just the first", %{
      source: source
    } do
      ids = Enum.map(1..3, fn _ -> Ecto.UUID.generate() end)
      messages = [segment_message(Enum.map(ids, &record(source.id, &1)))]

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, _source ->
        send(pid, {:dispatched, Enum.map(event_params, & &1["id"])})
        {:ok, length(event_params)}
      end)

      ConsumerPipeline.handle_batch(:default, messages, %{}, %{})

      assert_receive {:dispatched, dispatched_ids}
      assert MapSet.new(dispatched_ids) == MapSet.new(ids)
    end

    test "preserves original event IDs from the body field", %{source: source} do
      id1 = Ecto.UUID.generate()
      id2 = Ecto.UUID.generate()
      messages = [line_message(source.id, id1), line_message(source.id, id2)]

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, _source ->
        send(pid, {:dispatched, Enum.map(event_params, & &1["id"])})
        {:ok, length(event_params)}
      end)

      ConsumerPipeline.handle_batch(:default, messages, %{}, %{})

      assert_receive {:dispatched, ids}
      assert MapSet.new(ids) == MapSet.new([id1, id2])
    end

    test "routes events to the correct source when a batch spans multiple sources", %{source: s1} do
      user2 = insert(:user)
      s2 = insert(:source, user: user2)

      id1 = Ecto.UUID.generate()
      id2 = Ecto.UUID.generate()
      messages = [line_message(s1.id, id1), line_message(s2.id, id2)]

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, source ->
        send(pid, {:dispatched, source.id, Enum.map(event_params, & &1["id"])})
        {:ok, length(event_params)}
      end)

      ConsumerPipeline.handle_batch(:default, messages, %{}, %{})

      dispatched =
        1..2
        |> Enum.reduce_while(%{}, fn _, acc ->
          receive do
            {:dispatched, sid, ids} -> {:cont, Map.put(acc, sid, ids)}
          after
            500 -> {:halt, acc}
          end
        end)

      assert dispatched[s1.id] == [id1]
      assert dispatched[s2.id] == [id2]
    end

    test "skips events with an unknown source_id, emitting skipped telemetry", %{source: _source} do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :skipped])

      messages = [line_message(999_999_999, Ecto.UUID.generate())]

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, source ->
        send(pid, {:dispatched, event_params, source.id})
        {:ok, length(event_params)}
      end)

      ConsumerPipeline.handle_batch(:default, messages, %{}, %{})

      refute_receive {:dispatched, _, _}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :consumer, :skipped],
                      %{count: 1}, %{reason: :unknown_source_id}}
    end

    test "skips events with a nil source_id, emitting skipped telemetry", %{source: _source} do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :skipped])

      message =
        segment_message([
          %{
            "source_id" => nil,
            "body" => %{"id" => Ecto.UUID.generate()},
            "event_type" => "log"
          }
        ])

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, source ->
        send(pid, {:dispatched, event_params, source.id})
        {:ok, length(event_params)}
      end)

      ConsumerPipeline.handle_batch(:default, [message], %{}, %{})

      refute_receive {:dispatched, _, _}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :consumer, :skipped],
                      %{count: 1}, %{reason: :missing_source_id}}
    end

    test "a raising dispatch fails only that source's messages", %{source: source} do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :skipped])

      user = insert(:user)
      other_source = insert(:source, user: user)

      poison = line_message(source.id, Ecto.UUID.generate())
      healthy = line_message(other_source.id, Ecto.UUID.generate())

      pid = self()

      stub(Logflare.Backends, :dispatch_from_spool, fn event_params, dispatched_source ->
        if dispatched_source.id == source.id do
          raise FunctionClauseError
        end

        send(pid, {:dispatched, dispatched_source.id})
        {:ok, length(event_params)}
      end)

      assert [returned_poison, returned_healthy] =
               ConsumerPipeline.handle_batch(:default, [poison, healthy], %{}, %{})

      assert returned_poison.status == {:failed, :dispatch_error}
      assert returned_healthy.status == :ok

      assert_receive {:dispatched, dispatched_id}
      assert dispatched_id == other_source.id

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :consumer, :skipped],
                      %{count: 1}, %{reason: :dispatch_error}}
    end
  end

  describe "ack/3" do
    test "emits messages_failed telemetry when Broadway marks messages as failed" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :messages_failed])

      failed = [
        %Message{
          data: [],
          acknowledger: {ConsumerPipeline, :noop, nil},
          status: {:failed, :boom}
        }
      ]

      assert ConsumerPipeline.ack(:ref, [], failed) == :ok

      assert_receive {:telemetry_event,
                      [:logflare, :backends, :spool, :consumer, :messages_failed], %{count: 1},
                      %{}}
    end

    test "emits no telemetry when there are no failed messages" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :messages_failed])

      assert ConsumerPipeline.ack(:ref, [%Message{data: [], acknowledger: nil}], []) == :ok

      refute_receive {:telemetry_event,
                      [:logflare, :backends, :spool, :consumer, :messages_failed], _, _}
    end

    test "returns each message's bytes (not its count) to the producer's in-flight budget" do
      ref = :atomics.new(1, signed: true)
      :atomics.add(ref, 1, 300)

      successful = [byte_message(ref, 100)]
      failed = [%{byte_message(ref, 200) | status: {:failed, :boom}}]

      assert ConsumerPipeline.ack(:ref, successful, failed) == :ok

      # Both successful and failed messages give their bytes back, or the
      # producer's budget would leak until it stopped emitting entirely.
      assert :atomics.get(ref, 1) == 0
    end

    test "tolerates messages with no in-flight ref" do
      assert ConsumerPipeline.ack(
               :ref,
               [%Message{data: [], acknowledger: {ConsumerPipeline, :noop, nil}}],
               []
             ) == :ok
    end
  end

  defp byte_message(ref, bytes) do
    %Message{
      data: [],
      acknowledger: {ConsumerPipeline, :noop, %{in_flight_ref: ref, bytes: bytes}}
    }
  end
end
