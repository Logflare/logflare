defmodule Logflare.Source.BigQuery.BufferCounter do
  @moduledoc """
  Maintains a count of log events inside the Source.BigQuery.Pipeline Broadway pipeline.
  """

  use GenServer
  alias Logflare.LogEvent, as: LE
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source
  alias Logflare.PubSubRates
  alias Logflare.Source.BigQuery.Pipeline

  require Logger

  @table :buffer_counters
  @broadcast_every 5_000
  @max_buffer_len 5_000
  @pool_size Application.compile_env(:logflare, Logflare.PubSub)[:pool_size]

  def start_link(%RLS{} = rls) do
    start_link(source_token: rls.source_id)
  end

  def start_link(opts) do
    source_token = Keyword.get(opts, :source_token)

    GenServer.start_link(
      __MODULE__,
      opts,
      name: Source.Supervisor.via(__MODULE__, source_token)
    )
  end

  def init([source_token: source_token] = opts) do
    ensure_ets_key(source_token)
    Process.flag(:trap_exit, true)
    loop()
    check_buffer()
    state = Enum.into(opts, %{})
    {:ok, state}
  end

  @doc """
  Wraps `LogEvent`s in a `Broadway.Message`, pushes log events into the Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push(LE.t()) :: {:ok, map()} | {:error, :buffer_full}
  def push(%LE{source: %Source{token: source_token} = source} = le) do
    batch = [
      %Broadway.Message{
        data: le,
        acknowledger: {Source.BigQuery.BufferProducer, source_token, nil}
      }
    ]

    push_batch(source, batch)
  end

  @doc """
  Takes a `batch` of `Broadway.Message`s, pushes them into a Broadway pipeline and increments the `BufferCounter` count.
  """

  @spec push_batch(Source.t(), [Broadway.Message.t(), ...]) :: :ok | {:error, :buffer_full}
  def push_batch(%Source{token: source_token}, batch)
      when is_list(batch) and is_atom(source_token) do
    count = Enum.count(batch)
    # increment counter by x amount
    ensure_ets_key(source_token)

    with {:ok, _new_len} <- push_by(source_token, count),
         _ <- :ets.update_counter(@table, source_token, {2, count}) do
      Pipeline.name(source_token)
      |> Broadway.push_messages(batch)

      :ok
    end
  end

  @doc """
  Decrements the actual buffer count. If we've got a successfull `ack` from Broadway it means
  we don't have the log event anymore.
  """
  @spec ack(atom(), binary()) :: :ok
  def ack(source_token, log_event_id) when is_binary(log_event_id) do
    :ets.update_counter(@table, source_token, {2, -1})
    :ok
  end

  @spec ack_batch(atom(), [Broadway.Message.t()]) :: :ok
  def ack_batch(source_token, log_events) when is_list(log_events) do
    ensure_ets_key(source_token)
    count = Enum.count(log_events)
    :ets.update_counter(@table, source_token, {2, -count})
    :ok
  end

  @spec push_by(atom(), integer) ::
          {:error, :buffer_full} | {:ok, integer()}
  def push_by(source_token, count) do
    len = len(source_token)

    # allow bursting
    if len <= @max_buffer_len do
      {:ok, len + count}
    else
      {:error, :buffer_full}
    end
  end

  @spec set_len(atom(), non_neg_integer()) :: :ok
  def set_len(source_token, num) do
    :ets.update_element(@table, source_token, {2, num})
    :ok
  end

  @doc """
  Gets the current length of the buffer.
  """
  @spec len(Source.t()) :: integer
  def len(%Source{token: source_token}), do: len(source_token)

  def len(source_token) when is_atom(source_token) do
    :ets.lookup_element(@table, source_token, 2, 0)
  end

  def handle_info(:loop, state) do
    pipeline_name = Pipeline.name(state.source_token)

    producer = Broadway.producer_names(pipeline_name) |> List.first()
    count = GenStage.estimate_buffered_count(producer)
    set_len(state.source_token, count)
    loop()

    {:noreply, state}
  end

  def handle_info(:check_buffer, state) do
    if Source.RateCounterServer.should_broadcast?(state.source_token) do
      broadcast_buffer(state.source_token)
    end

    check_buffer()

    {:noreply, state}
  end

  def terminate(reason, state) do
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{
      source_id: state.source_token,
      source_Token: state.source_token
    })

    reason
  end

  defp broadcast_buffer(source_token) when is_atom(source_token) do
    len = len(source_token)
    local_buffer = %{Node.self() => %{len: len}}

    shard = :erlang.phash2(source_token, @pool_size)

    Phoenix.PubSub.broadcast(
      Logflare.PubSub,
      "buffers:shard-#{shard}",
      {:buffers, source_token, local_buffer}
    )

    cluster_buffer = PubSubRates.Cache.get_cluster_buffers(source_token)

    payload = %{
      buffer: cluster_buffer,
      source_token: source_token
    }

    Source.ChannelTopics.broadcast_buffer(payload)
  end

  defp check_buffer() do
    Process.send_after(self(), :check_buffer, @broadcast_every)
  end

  defp loop do
    Process.send_after(self(), :loop, 1_000)
  end

  defp ensure_ets_key(source_token) do
    :ets.insert_new(@table, {source_token, 0})
  end
end
