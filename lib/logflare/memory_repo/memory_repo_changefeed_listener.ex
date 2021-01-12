defmodule Logflare.MemoryRepo.ChangefeedsListener do
  use Logflare.Commons
  use GenServer
  require Logger
  import Ecto.Query

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
      id: __MODULE__,
      start: {__MODULE__, :start_link, args}
    }
  end

  @spec start_link([String.t()], [any]) :: {:ok, pid}
  def start_link(args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def init(channels) do
    results =
      for channel <- channels do
        Logger.debug("Starting #{__MODULE__} with channel subscription: #{channel}")
        {:ok, pid} = Postgrex.Notifications.start_link(Repo.config())
        {:ok, ref} = Postgrex.Notifications.listen(pid, channel)
        {pid, channel, ref}
      end

    {:ok, results}
  end

  @doc """
  Handle changefeed notification
  """
  def handle_info({:notification, _pid, _ref, _channel_name, payload}, _state) do
    payload
    |> Jason.decode!()
    |> process_notification()

    {:noreply, :event_handled}
  catch
    huh, error ->
      Logger.error("Cache invalidation worker error: #{inspect(error)}")
      {:noreply, :event_error}
  end

  def handle_info(_value, _state) do
    {:noreply, :event_received}
  end

  def process_notification(%{"type" => "DELETE"} = payload) do
    chfd_event = ChangefeedEvent.build(payload)

    schema = MemoryRepo.table_to_schema(chfd_event.table)

    {1, nil} = MemoryRepo.delete_all(from(schema) |> where([t], t.id == ^chfd_event.id))
  end

  def process_notification(%{"type" => type} = payload) when type in ["UPDATE", "INSERT"] do
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
