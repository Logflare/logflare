defmodule Logflare.Test.RegistryShutdownClient do
  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: {:via, Registry, {opts[:registry], opts[:key]}})
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)
    {:ok, _} = Registry.register(opts[:in_flight_registry], self(), :in_flight)
    {:ok, Map.new(opts)}
  end

  @impl GenServer
  def handle_info({:EXIT, pid, reason}, state) do
    send(state.owner, {:registry_exit, self(), pid, reason})
    {:noreply, state}
  end

  @impl GenServer
  def terminate(reason, state) do
    registration = Registry.lookup(state.registry, state.key)
    in_flight = Registry.lookup(state.in_flight_registry, self())
    send(state.owner, {:terminated, self(), reason, registration, in_flight})
  end
end
