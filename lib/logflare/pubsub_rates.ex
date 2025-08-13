defmodule Logflare.PubSubRates do
  @moduledoc false
  use Supervisor

  alias Logflare.PubSubRates
  alias Phoenix.PubSub

  @topics ["buffers", "rates", "inserts"]

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      PubSubRates.Cache,
      {PartitionSupervisor,
       child_spec: PubSubRates.Rates,
       name: PubSubRates.Rates.Supervisors,
       partitions: partitions(),
       with_arguments: fn [opts], partition ->
         [Keyword.put(opts, :partition, Integer.to_string(partition))]
       end},
      {PartitionSupervisor,
       child_spec: PubSubRates.Buffers,
       name: PubSubRates.Buffers.Supervisors,
       partitions: partitions(),
       with_arguments: fn [opts], partition ->
         [Keyword.put(opts, :partition, Integer.to_string(partition))]
       end},
      {PartitionSupervisor,
       child_spec: PubSubRates.Inserts,
       name: PubSubRates.Inserts.Supervisors,
       partitions: partitions(),
       with_arguments: fn [opts], partition ->
         [Keyword.put(opts, :partition, Integer.to_string(partition))]
       end}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Subscribe to all rate topics

  ### Examples

    iex> subscribe(:all)
    iex> subscribe(["buffers", "inserts", "rates])
  """
  @spec subscribe(:all | binary() | maybe_improper_list()) ::
          :ok | list() | {:error, {:already_registered, pid()}}

  def subscribe(:all), do: subscribe(@topics)

  def subscribe(topics) when is_list(topics) do
    for topic <- topics, partition <- 0..partitions() do
      part = Integer.to_string(partition)
      subscribe(topic, part)
    end
  end

  @doc """
  Subscribes to a topic for a partition.

  ### Examples

    iex> subscribe("buffers", "0")
    iex> subscribe("inserts", "56")
  """
  @spec subscribe(binary(), binary()) :: :ok | {:error, {:already_registered, pid()}}
  def subscribe(topic, partition) when topic in @topics and is_binary(partition) do
    PubSub.subscribe(Logflare.PubSub, topic <> partition)
  end

  @doc """
  Global sharded broadcast for a rate-specific message.
  """
  @spec global_broadcast_rate(
          {binary(), any(), any()}
          | {binary(), integer(), any(), any()}
        ) :: :ok | {:error, any()}

  def global_broadcast_rate({topic, source_id, backend_id, _payload} = data)
      when topic in @topics do
    partitioned_topic = partitioned_topic(topic, {source_id, backend_id})
    Phoenix.PubSub.broadcast(Logflare.PubSub, partitioned_topic, data)
  end

  def global_broadcast_rate({topic, source_token, _payload} = data)
      when topic in @topics do
    partitioned_topic = partitioned_topic(topic, source_token)

    Phoenix.PubSub.broadcast(Logflare.PubSub, partitioned_topic, data)
  end

  @doc """
  The number of partitions for a paritioned child.
  """
  @spec partitions() :: integer()
  def partitions, do: Application.get_env(:logflare, Logflare.PubSub)[:pool_size]

  @doc """
  Partitions a topic for a key.
  """
  @spec partitioned_topic(binary(), any()) :: binary()
  def partitioned_topic(topic, key) when is_binary(topic) do
    topic <> make_partition(key)
  end

  @doc """
  Makes a string of a partition integer from a key.
  """
  @spec make_partition(any()) :: binary()
  def make_partition(key) do
    :erlang.phash2(key, partitions()) |> Integer.to_string()
  end
end
