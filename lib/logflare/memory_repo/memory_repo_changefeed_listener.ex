defmodule Logflare.MemoryRepo.ChangefeedListener do
  use Logflare.Commons
  use GenServer
  require Logger
  import Ecto.Query

  @id_only_changefeed_suffix "_id_only_changefeed"
  @id_only_changefeed_suffix_byte_size byte_size(@id_only_changefeed_suffix)

  defguardp changefeed_with_id_only?(channel)
            when binary_part(
                   channel,
                   byte_size(channel) - @id_only_changefeed_suffix_byte_size,
                   @id_only_changefeed_suffix_byte_size
                 ) == "_id_only_changefeed"

  defmodule ChangefeedEvent do
    use TypedStruct

    typedstruct do
      field :id, term()
      field :old, map()
      field :new, map()
      field :table, String.t()
      field :type, String.t()
    end

    def build(attrs) do
      struct!(__MODULE__, for({k, v} <- attrs, do: {String.to_atom(k), v}))
    end
  end

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
    process_notification(channel_name, Jason.decode!(payload))

    {:noreply, :event_handled}
  catch
    huh, error ->
      Logger.error("Cache invalidation worker error: #{inspect(error)}")
      {:noreply, :event_error}
  end

  def handle_info(_value, _state) do
    {:noreply, :event_received}
  end

  def process_notification(_, %{"type" => "DELETE"} = payload) do
    chfd_event = ChangefeedEvent.build(payload)

    schema = MemoryRepo.table_to_schema(chfd_event.table)

    {1, nil} = MemoryRepo.delete_all(from(schema) |> where([t], t.id == ^chfd_event.id))
  end

  def process_notification(
        channel_name,
        %{"id" => id, "type" => type, "table" => table}
      )
      when type in ["UPDATE", "INSERT"] and changefeed_with_id_only?(channel_name) do
    schema = MemoryRepo.table_to_schema(table)
    struct = Repo.get(schema, String.to_integer(id))

    {:ok, _} =
      MemoryRepo.insert(struct,
        on_conflict: :replace_all,
        conflict_target: :id
      )
  end

  def process_notification(_channel_name, %{"type" => type} = payload)
      when type in ["UPDATE", "INSERT"] do
    chfd_event = ChangefeedEvent.build(payload)

    schema = MemoryRepo.table_to_schema(chfd_event.table)
    changeset = to_changeset(schema, chfd_event)

    {:ok, _} =
      MemoryRepo.insert(changeset,
        on_conflict: :replace_all,
        conflict_target: :id
      )
  end

  def to_changeset(schema, chfd_event) do
    schema.changefeed_changeset(chfd_event.new)
  end
end
