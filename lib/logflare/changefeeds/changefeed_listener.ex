defmodule Logflare.Changefeeds.ChangefeedListener do
  use Logflare.Commons
  alias Logflare.Changefeeds.ChangefeedEvent
  use GenServer
  require Logger
  import Ecto.Query
  @operations_type [:update, :insert, :delete]

  defguardp changefeed_with_id_only?(event) when event.changefeed_subscription.id_only == true

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
  def handle_info({:notification, _pid, _ref, _channel_name, payload}, _state) do
    payload
    |> Jason.decode!()
    |> ChangefeedEvent.build()
    |> process_notification()

    {:noreply, :event_handled}
  rescue
    error ->
      Logger.error("Cache invalidation worker error: #{inspect(error)}")
      {:noreply, :event_error}
  end

  def handle_info(_value, _state) do
    {:noreply, :event_received}
  end

  def process_notification(%{type: :delete, node_id: origin_node} = chfd_event)
      when node() != origin_node do
    schema = chfd_event.changefeed_subscription.schema

    {1, nil} = LocalRepo.delete_all(from(schema) |> where([t], t.id == ^chfd_event.id))
  end

  def process_notification(%{type: :insert, node_id: origin_node} = chfd_event)
      when not changefeed_with_id_only?(chfd_event) and node() != origin_node do
    changeset = to_changeset(chfd_event)

    {:ok, struct} = LocalRepo.insert(changeset, on_conflict: :replace_all, conflict_target: :id)

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(%{type: :update, changes: changes, node_id: origin_node} = chfd_event)
      when not is_nil(changes) and node() != origin_node do
    schema = chfd_event.changefeed_subscription.schema
    struct = LocalRepo.get(schema, chfd_event.id)

    changeset =
      struct
      |> to_changeset(chfd_event)
      |> Ecto.Changeset.force_change(:updated_at, struct.updated_at)
      |> Ecto.Changeset.force_change(:inserted_at, struct.inserted_at)

    {:ok, struct} = LocalRepo.update(changeset)

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(
        %{id: id, type: type, table: _table, node_id: origin_node} = chfd_event
      )
      when type in [:update, :insert] and changefeed_with_id_only?(chfd_event) and
             node() != origin_node do
    schema = chfd_event.changefeed_subscription.schema
    struct = Repo.get(schema, id) |> Changefeeds.replace_assocs_with_nils()

    {:ok, struct} =
      LocalRepo.insert(struct,
        on_conflict: :replace_all,
        conflict_target: :id
      )

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(%{type: type, node_id: origin_node} = chfd_event)
      when type in [:update, :insert] and node() != origin_node do
    changeset = to_changeset(chfd_event)

    {:ok, struct} =
      LocalRepo.insert(changeset,
        on_conflict: :replace_all,
        conflict_target: :id
      )

    Changefeeds.maybe_insert_virtual(struct)
  end

  def process_notification(%{node_id: origin_node} = _ev) when node() == origin_node do
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
