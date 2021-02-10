defmodule Logflare.ChangefeedsListenerTest do
  @moduledoc false
  use Logflare.DataCase
  use Logflare.Commons
  alias Logflare.MemoryRepo.ChangefeedListener

  describe "Changefeed listeners" do
    test "notifications from origin node are not processed" do
      for type <- ChangefeedListener.operations_types(),
          id_only <- [true, false],
          changes <- [nil, %{}] do
        chfd_event = %Changefeeds.ChangefeedEvent{
          id: 1,
          type: type,
          node_id: Node.self(),
          table: "table",
          changes: changes,
          changefeed_subscription: %Changefeeds.ChangefeedSubscription{
            table: "table",
            schema: Schema,
            id_only: id_only
          }
        }

        channel =
          if id_only do
            "table_id_only_changefeed"
          else
            "table_changefeed"
          end

        assert MemoryRepo.ChangefeedListener.process_notification(channel, chfd_event) == :noop
      end
    end

    test "notifications from other nodes are processed" do
      for type <- ChangefeedListener.operations_types(),
          id_only <- [true, false],
          changes <- [nil, %{}] do
        chfd_event = %Changefeeds.ChangefeedEvent{
          id: 1,
          type: type,
          node_id: "10.0.0.0@logflare",
          table: "table",
          changes: changes,
          changefeed_subscription: %Changefeeds.ChangefeedSubscription{
            table: "table",
            schema: NonExistingSchema,
            id_only: id_only
          }
        }

        channel =
          if id_only do
            "table_id_only_changefeed"
          else
            "table_changefeed"
          end

        assert catch_error(
                 MemoryRepo.ChangefeedListener.process_notification(channel, chfd_event)
               )
      end
    end
  end
end
