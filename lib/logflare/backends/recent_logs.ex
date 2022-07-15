defmodule Logflare.Backends.RecentLogs do
  @doc """
  A distributed cache of latest 100 logs.

  Listens to the cluster-wide recent_logs channel topic.

  Periodically does a cluster sync to ensure all local caches are syncronised.
  """
  alias Logflare.{Source, Backends, LogEvent}
  use TypedStruct
  use GenServer

  typedstruct enforce: true do
    field :source, Source.t()
    field :data, list(LogEvent.t())
  end

  def start_link(%Source{id: id} = source) do
    GenServer.start_link(__MODULE__, source, name: Backends.via_source(source, __MODULE__))
  end

  def init(source) do
    {:ok, %__MODULE__{source: source, data: []}}
  end

  @doc """
  Pushes events into the cache, and removes older events.
  """
  @spec push(pid(), list(LogEvent.t())) :: :ok
  def push(pid, events) when is_list(events) do
    GenServer.cast(pid, {:push, events})
  end

  @doc """
  Returns the list of cached log events, sorted.
  """
  @spec push(pid(), list(LogEvent.t())) :: :ok
  def push(pid, events) do
    GenServer.cast(pid, {:push, events})
  end

  def handle_cast({:push, log_events}, state) when is_list(log_events) do
    sorted = Enum.sort_by(log_events, & &1.body.timestamp)

    data =
      (sorted ++ state.data)
      |> Enum.take(100)

    {:noreply, %{state | data: data}}
  end

  def handle_call(list, _from, state) do
    {:reply, state.data, state}
  end
end
