defmodule Logflare.Buffers.MemoryBuffer do
  @moduledoc """
  This is an implementation of an in-memory buffer, using `:queue`.any()
  All operations are synchronous.
  """

  @behaviour Logflare.Buffers.Buffer

  use GenServer
  use TypedStruct

  # API

  @impl Logflare.Buffers.Buffer
  def add_many(identifier, payloads) do
    GenServer.call(identifier, {:add, payloads})
  end

  @impl Logflare.Buffers.Buffer
  def pop_many(identifier, number) do
    GenServer.call(identifier, {:pop, number})
  end

  @impl Logflare.Buffers.Buffer
  def clear(identifier) do
    GenServer.call(identifier, :clear)
  end

  @impl Logflare.Buffers.Buffer
  def length(identifier) do
    GenServer.call(identifier, :length)
  end

  # GenServer state and init callbacks
  typedstruct module: State do
    @moduledoc false

    field :queue, :queue.queue() | nil, default: :queue.new()
    field :proc_name, atom() | binary() | pid(), enforce: true
    field :size, non_neg_integer(), default: 0
  end

  def start_link(opts \\ []) do
    proc_name = opts[:proc_name] || opts[:name]

    GenServer.start_link(__MODULE__, %{proc_name: proc_name}, opts)
  end

  @impl GenServer
  def init(%{proc_name: proc_name}) do
    {:ok, %State{proc_name: proc_name || self()}}
  end

  # GenServer callbacks

  @impl GenServer
  def handle_call({:add, payloads}, _from, state) do
    to_join = :queue.from_list(payloads)
    new_queue = :queue.join(state.queue, to_join)
    {:reply, :ok, %State{state | queue: new_queue}}
  end

  def handle_call({:pop, number}, _from, state) do
    {items, new_queue} =
      case :queue.len(state.queue) do
        n when n == 0 ->
          {[], state.queue}

        n when n - number <= 0 ->
          {:queue.to_list(state.queue), :queue.new()}

        _n ->
          {popped_queue, queue} = :queue.split(number, state.queue)
          {:queue.to_list(popped_queue), queue}
      end

    {:reply, {:ok, items}, %State{state | queue: new_queue}}
  end

  def handle_call(:clear, _from, state) do
    {:reply, :ok, %State{state | queue: :queue.new()}}
  end

  def handle_call(:length, _from, state) do
    {:reply, :queue.len(state.queue), state}
  end
end
