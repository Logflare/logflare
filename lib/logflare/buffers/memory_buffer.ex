defmodule Logflare.Buffers.MemoryBuffer do
  @moduledoc """
  This is an implementation of an in-memory buffer, using `:queue`.any()
  All operations are syncronous.
  """
  alias Logflare.{Buffers.Buffer, Buffers.MemoryBuffer}
  use GenServer
  @behaviour Buffer

  # GenServer state and init callbacks
  defstruct queue: nil

  def start_link(_) do
    GenServer.start_link(__MODULE__, [])
  end

  @impl true
  def init(_) do
    {:ok, %MemoryBuffer{queue: :queue.new()}}
  end

  # API

  @impl Buffer
  def add(identifier, payload) do
    GenServer.call(identifier, {:add, [payload]})
  end

  @impl Buffer
  def add_many(identifier, payloads) do
    GenServer.call(identifier, {:add, payloads})
  end

  @impl Buffer
  def pop(identifier) do
    GenServer.call(identifier, {:pop, 1})
  end

  @impl Buffer
  def pop_many(identifier, number) do
    GenServer.call(identifier, {:pop, number})
  end

  @impl Buffer
  def clear(identifier) do
    GenServer.call(identifier, :clear)
  end

  @impl Buffer
  def length(identifier) do
    GenServer.call(identifier, :length)
  end

  # GenServer callbacks

  @impl true
  def handle_call({:add, payloads}, _from, state) do
    to_join = :queue.from_list(payloads)
    new_queue = :queue.join(state.queue, to_join)
    {:reply, :ok, %{state | queue: new_queue}}
  end

  @impl true
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

    {:reply, {:ok, items}, %{state | queue: new_queue}}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    {:reply, :ok, %{state | queue: :queue.new()}}
  end

  @impl true
  def handle_call(:length, _from, state) do
    {:reply, :queue.len(state.queue), state}
  end
end
