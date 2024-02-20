defmodule Logflare.Cluster.CacheWarmer do
  @moduledoc """
  Performs cross-node cache warming, by retrieving all cache data from the other node and setting it on the cache.


  """
  use Cachex.Warmer
  import Cachex.Spec
  @agent __MODULE__.State

  def handle_info({:batch, cache, pairs}, state) do
    Cachex.import(cache, pairs)
    {:noreply, state}
  end

  # only on startup
  @impl Cachex.Warmer
  def interval, do: :timer.hours(24 * 365)
  @impl Cachex.Warmer
  def execute(cache) do
    # starts the agent if not yet started
    # if already started, will return error tuple, but does not affect subsequent Agent.get
    # use agent to store state as GenServer state is managed by Cachex.
    Agent.start_link(fn -> %{node: nil} end, name: @agent)
    prev_node = Agent.get(@agent, & &1.node)

    target =
      if prev_node != nil do
        prev_node
      else
        Node.list()
        |> Enum.map(fn node ->
          case :rpc.call(node, Cachex, :count, [cache]) do
            {:ok, count} when count > 0 -> node
            _ -> nil
          end
        end)
        |> Enum.filter(&(&1 != nil))
        |> Enum.sort_by(&Atom.to_string/1)
        |> List.first()
      end

    if target do
      pid = self()

      # don't block the caller, using :rpc.async_call results in crashloop due to key message handling.
      Task.start(fn ->
        :rpc.call(target, __MODULE__, :stream_to_node, [pid, cache])
      end)
    end

    {:ok, []}
  end

  # stream entries to the provided target node
  def stream_to_node(pid, cache) do
    # send message to CacheWarmer process on that node
    Cachex.stream!(cache)
    |> Stream.chunk_every(250)
    |> Stream.each(fn chunk ->
      send(pid, {:batch, cache, chunk})
    end)
    |> Stream.run()

    :ok
  end

  def warmer_spec(mod) do
    warmer(module: __MODULE__, state: mod)
  end
end
