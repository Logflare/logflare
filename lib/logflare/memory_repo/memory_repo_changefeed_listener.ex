defmodule Logflare.MemoryRepo.ChangefeedListener do
  use Logflare.Commons
  alias Logflare.Changefeeds.ChangefeedEvent
  use GenServer
  require Logger
  import Ecto.Query
  @operations_type ["UPDATE", "INSERT", "DELETE"]

  @id_only_changefeed_suffix "_id_only_changefeed"
  @id_only_changefeed_suffix_byte_size byte_size(@id_only_changefeed_suffix)

  defguardp changefeed_with_id_only?(channel)
            when binary_part(
                   channel,
                   byte_size(channel) - @id_only_changefeed_suffix_byte_size,
                   @id_only_changefeed_suffix_byte_size
                 ) == "_id_only_changefeed"

  def child_spec(args) do
    %{
      id: :"changefeed_listener_#{hd(args).changefeed}",
      start: {__MODULE__, :start_link, args}
    }
  end

  @spec start_link([String.t()], [any]) :: {:ok, pid}
  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  @impl true
  def init(%{notifications_pid: pid, changefeed: changefeed}) do
    Logger.debug("Starting #{__MODULE__} with channel subscription: #{changefeed}")
    {:ok, _ref} = Postgrex.Notifications.listen(pid, changefeed)

    {:ok, changefeed}
  end

  @doc """
  Handle changefeed notification
  """
  @impl true
  def handle_info({:notification, _pid, _ref, channel_name, payload}, _state) do
    chfd_event =
      payload
      |> Jason.decode!()
      |> ChangefeedEvent.build()

    process_notification(channel_name, chfd_event)

    {:noreply, :event_handled}
  catch
    huh, error ->
      Logger.error("Cache invalidation worker error: #{inspect(error)}")
      {:noreply, :event_error}
  end

  def handle_info(_value, _state) do
    {:noreply, :event_received}
  end

  def process_notification(_, %{type: "DELETE", node_id: origin_node} = chfd_event)
      when node() != origin_node do
    schema = chfd_event.changefeed_subscription.schema

    {1, nil} = MemoryRepo.delete_all(from(schema) |> where([t], t.id == ^chfd_event.id))
  end

  def process_notification(channel_name, %{type: "INSERT", node_id: origin_node} = chfd_event)
      when not changefeed_with_id_only?(channel_name) and node() != origin_node do
    changeset = to_changeset(chfd_event)

    {:ok, struct} = MemoryRepo.insert(changeset, on_conflict: :replace_all, conflict_target: :id)

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(
        _channel_name,
        %{type: "UPDATE", changes: changes, node_id: origin_node} = chfd_event
      )
      when not is_nil(changes) and node != origin_node do
    schema = chfd_event.changefeed_subscription.schema
    struct = MemoryRepo.get(schema, chfd_event.id)
    changeset = to_changeset(struct, chfd_event)
    changeset = Ecto.Changeset.force_change(changeset, :updated_at, struct.updated_at)
    changeset = Ecto.Changeset.force_change(changeset, :inserted_at, struct.inserted_at)

    {:ok, struct} = MemoryRepo.update(changeset)

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(
        channel_name,
        %{id: id, type: type, table: _table, node_id: origin_node} = chfd_event
      )
      when type in ["UPDATE", "INSERT"] and changefeed_with_id_only?(channel_name) and
             node != origin_node do
    schema = chfd_event.changefeed_subscription.schema
    struct = Repo.get(schema, id) |> Changefeeds.replace_assocs_with_nils()

    {:ok, struct} =
      MemoryRepo.insert(struct,
        on_conflict: :replace_all,
        conflict_target: :id
      )

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(_channel_name, %{type: type, node_id: origin_node} = chfd_event)
      when type in ["UPDATE", "INSERT"] and node() != origin_node do
    # schema = chfd_event.changefeed_subscription.schema
    changeset = to_changeset(chfd_event)

    {:ok, struct} =
      MemoryRepo.insert(changeset,
        on_conflict: :replace_all,
        conflict_target: :id
      )

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(_channel_name, %{node_id: origin_node}) when node() == origin_node do
    :noop
  end

  def to_changeset(chfd_event) do
    chfd_event.changefeed_subscription.schema.changefeed_changeset(chfd_event.changes)
  end

  def to_changeset(struct, chfd_event) do
    chfd_event.changefeed_subscription.schema.changefeed_changeset(struct, chfd_event.changes)
  end

  def operations_types() do
    @operations_type
  end
end
