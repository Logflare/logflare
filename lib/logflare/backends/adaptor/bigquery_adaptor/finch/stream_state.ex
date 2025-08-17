defmodule GRPC.Client.Adapters.Finch.StreamState do
  use GenServer

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, :ok)
  end

  def add_item(pid, item) do
    GenServer.cast(pid, {:add_item, item})
  end

  def close(pid) do
    GenServer.cast(pid, :close)
  end

  def next_item(pid) do
    GenServer.call(pid, :next_item)
  end

  @impl true
  def init(:ok) do
    initial_state = %{items: :queue.new(), from: nil}
    {:ok, initial_state}
  end

  @impl true
  def handle_cast({:add_item, item}, state) do
    new_queue = :queue.in(item, state.items)
    {:noreply, %{state | items: new_queue}, {:continue, :response}}
  end

  @impl true
  def handle_cast(:close, state) do
    new_queue = :queue.in(:close, state.items)
    {:noreply, %{state | items: new_queue}, {:continue, :response}}
  end

  @impl true
  def handle_call(:next_item, from, state) do
    {:noreply, %{state | from: from}, {:continue, :response}}
  end

  @impl true
  def handle_continue(:response, state) do
    no_items? = :queue.is_empty(state.items)
    without_from? = is_nil(state.from)

    cond do
      without_from? ->
        {:noreply, state}

      no_items? ->
        {:noreply, state}

      true ->
        case :queue.out(state.items) do
          {{:value, :close}, _} ->
            GenServer.reply(state.from, :close)
            {:stop, :normal, state}

          {{:value, item}, new_queue} ->
            GenServer.reply(state.from, item)
            {:noreply, %{state | from: nil, items: new_queue}}
        end
    end
  end
end
