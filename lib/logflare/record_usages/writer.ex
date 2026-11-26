defmodule Logflare.RecordUsages.Writer do
  @moduledoc """
  Writes usage timestamps to Cachex one mailbox message at a time.
  Drops samples when the mailbox reaches 1,000 messages. Concurrent callers can
  overshoot this threshold before their casts arrive.
  Failed writes are discarded. Mailbox messages are best effort and are lost on restart.
  """

  use GenServer

  require Logger

  alias Logflare.RecordUsages.Cache

  @max_mailbox_len 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
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
  @spec init(nil) :: {:ok, nil}
  def init(nil) do
    {:ok, nil}
  end

  @impl GenServer
  def handle_cast({:record, key, timestamp}, state) do
    case write(key, timestamp) do
      {:ok, _result} ->
        :ok

      {:error, reason} ->
        Logger.warning("Record usage cache write failed", error_string: inspect(reason))
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_call(:drain, _from, state) do
    {:reply, :ok, state}
  end

  @spec write(Cache.key(), integer()) :: {:ok, term()} | {:error, term()}
  defp write(key, timestamp) do
    Cache.put_if_newer(key, timestamp)
  rescue
    _error in [ArgumentError, Cachex.Error] -> {:error, :cache_unavailable}
  catch
    :exit, {reason, {GenServer, :call, _args}} -> {:error, reason}
  end
end
