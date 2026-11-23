defmodule Logflare.RecordUsages.Writer do
  @moduledoc """
  Coalesces usage timestamps while one linked batch task writes to Cachex.
  Drops samples when the mailbox reaches 1,000 messages. Concurrent callers can
  overshoot this threshold before their casts arrive.
  Pending timestamps and mailbox messages are best effort and are lost on restart.
  """

  use GenServer

  alias Logflare.RecordUsages.Cache

  @write_interval 100
  @max_mailbox_len 1_000

  @type timestamps :: %{Cache.key() => integer()}
  @type state :: %{
          pending: timestamps(),
          inflight: timestamps(),
          task: Task.t() | nil,
          waiters: [GenServer.from()],
          pending_waiters: [GenServer.from()]
        }

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @spec record(Cache.key(), integer()) :: :ok
  def record(key, timestamp) do
    with writer when is_pid(writer) <- Process.whereis(__MODULE__),
         {:message_queue_len, length} when length < @max_mailbox_len <-
           Process.info(writer, :message_queue_len) do
      GenServer.cast(writer, {:record, key, timestamp})
    end

    :ok
  end

  @spec drain() :: :ok
  def drain do
    GenServer.call(__MODULE__, :drain, :infinity)
  catch
    :exit, {_reason, {GenServer, :call, [__MODULE__, _request, _timeout]}} -> :ok
  end

  @impl GenServer
  @spec init(nil) :: {:ok, state()}
  def init(nil) do
    Process.flag(:trap_exit, true)
    Process.send_after(self(), :write, @write_interval)
    {:ok, %{pending: %{}, inflight: %{}, task: nil, waiters: [], pending_waiters: []}}
  end

  @impl GenServer
  def handle_cast({:record, key, timestamp}, state) do
    latest = max(Map.get(state.pending, key, timestamp), Map.get(state.inflight, key, timestamp))

    pending =
      if timestamp >= latest and Map.get(state.inflight, key) != timestamp do
        Map.put(state.pending, key, timestamp)
      else
        state.pending
      end

    {:noreply, %{state | pending: pending}}
  end

  @impl GenServer
  def handle_call(:drain, _from, %{pending: pending, task: nil} = state)
      when map_size(pending) == 0 do
    {:reply, :ok, state}
  end

  def handle_call(:drain, from, %{pending: pending} = state) when map_size(pending) > 0 do
    {:noreply, start_batch(%{state | pending_waiters: [from | state.pending_waiters]})}
  end

  def handle_call(:drain, from, state) do
    {:noreply, %{state | waiters: [from | state.waiters]}}
  end

  @impl GenServer
  def handle_info(:write, state) do
    Process.send_after(self(), :write, @write_interval)
    {:noreply, start_batch(state)}
  end

  def handle_info({ref, :ok}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, finish_batch(%{state | task: nil, inflight: %{}})}
  end

  def handle_info({ref, {:error, _reason}}, %{task: %Task{ref: ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, retry_batch(state)}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{task: %Task{ref: ref}} = state) do
    {:noreply, retry_batch(state)}
  end

  def handle_info({:EXIT, _pid, _reason}, state) do
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, %{task: nil}), do: :ok

  def terminate(_reason, %{task: task}) do
    Task.shutdown(task, :brutal_kill)
    :ok
  end

  @spec start_batch(state()) :: state()
  defp start_batch(%{task: nil, pending: pending} = state) when map_size(pending) > 0 do
    task = Task.async(fn -> write_batch(pending) end)

    %{
      state
      | task: task,
        inflight: pending,
        pending: %{},
        waiters: state.pending_waiters,
        pending_waiters: []
    }
  end

  defp start_batch(state), do: state

  @spec write_batch(timestamps()) :: :ok | {:error, term()}
  defp write_batch(entries) do
    Enum.each(entries, fn {key, timestamp} ->
      Cachex.get_and_update!(Cache, key, fn
        nil -> {:commit, timestamp}
        previous when previous < timestamp -> {:commit, timestamp}
        previous -> {:ignore, previous}
      end)
    end)
  rescue
    _error in [ArgumentError, Cachex.Error] -> {:error, :cache_unavailable}
  end

  @spec retry_batch(state()) :: state()
  defp retry_batch(state) do
    pending =
      Map.merge(state.pending, state.inflight, fn _key, left, right -> max(left, right) end)

    %{
      state
      | task: nil,
        inflight: %{},
        pending: pending,
        waiters: [],
        pending_waiters: state.waiters ++ state.pending_waiters
    }
  end

  @spec finish_batch(state()) :: state()
  defp finish_batch(state) do
    Enum.each(state.waiters, &GenServer.reply(&1, :ok))
    state = %{state | waiters: []}

    case state.pending_waiters do
      [] -> state
      _ -> start_batch(state)
    end
  end
end
