defmodule Logflare.Backends.Spool.ConsumerPipelineTest do
  use Logflare.DataCase, async: false

  import Mimic
  import Logflare.Factory

  alias Broadway.Message
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.TestUtils

  setup :set_mimic_global

  # Build a Broadway.Message as the pipeline's transform/2 would produce it —
  # data is a parsed NDJSON line map, acknowledger is the pipeline no-op.
  defp line_message(source_id, event_id, extra_body \\ %{}) do
    body =
      Map.merge(%{"id" => event_id, "timestamp" => System.system_time(:microsecond)}, extra_body)

    line = %{"source_id" => source_id, "body" => body, "id" => event_id, "event_type" => "log"}
    %Message{data: line, acknowledger: {ConsumerPipeline, :noop, nil}}
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

      message = %Message{
        data: %{
          "source_id" => nil,
          "body" => %{"id" => Ecto.UUID.generate()},
          "event_type" => "log"
        },
        acknowledger: {ConsumerPipeline, :noop, nil}
      }

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
  end

  describe "ack/3" do
    test "emits messages_failed telemetry when Broadway marks messages as failed" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :consumer, :messages_failed])

      failed = [
        %Message{
          data: %{},
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

      assert ConsumerPipeline.ack(:ref, [%Message{data: %{}, acknowledger: nil}], []) == :ok

      refute_receive {:telemetry_event,
                      [:logflare, :backends, :spool, :consumer, :messages_failed], _, _}
    end
  end
end
