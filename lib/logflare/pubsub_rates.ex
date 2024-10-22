defmodule Logflare.PubSubRates do
  @moduledoc false
  use Supervisor

  alias Logflare.PubSubRates
  alias Phoenix.PubSub
  @topics ["buffers", "rates", "inserts"]
  @partitions Application.get_env(:logflare, Logflare.PubSub)[:pool_size]

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      PubSubRates.Cache,
      {PartitionSupervisor,
       child_spec: PubSubRates.Rates,
       name: PubSubRates.Supervisors,
       partitions: partitions(),
       with_arguments: fn [opts], partition ->
         [Keyword.put(opts, :partition, Integer.to_string(partition))]
       end},
      PubSubRates.Buffers,
      PubSubRates.Inserts
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Subscribe to all rate topics

  ### Examples

    iex> subscribe(:all)
    iex> subscribe("buffers")
    iex> subscribe("inserts")
    iex> subscribe("rates")
  """
  @spec subscribe(:all | binary() | maybe_improper_list()) ::
          :ok | list() | {:error, {:already_registered, pid()}}

  def subscribe(:all), do: subscribe(@topics)

  def subscribe("rates" <> _partition = topic),
    do: PubSub.subscribe(Logflare.PubSub, topic)

  def subscribe(topics) when is_list(topics), do: Enum.map(topics, &subscribe/1)

  def subscribe(topic) when topic in @topics do
    PubSub.subscribe(Logflare.PubSub, topic)
  end

  @doc """
  Global sharded broadcast for a rate-specific message.
  """
  @spec global_broadcast_rate(
          {binary(), any(), any()}
          | {binary(), integer(), any(), any()}
        ) :: :ok | {:error, any()}

  def global_broadcast_rate({topic, source_id, _backend_id, _payload} = data)
      when topic in @topics and is_integer(source_id) do
    Phoenix.PubSub.broadcast(Logflare.PubSub, topic, data)
  end

  def global_broadcast_rate({"rates" = topic, source_token, _payload} = data)
      when topic in @topics do
    partitioned_topic = partitioned_topic(topic, source_token)

    Phoenix.PubSub.broadcast(Logflare.PubSub, partitioned_topic, data)
  end

  def global_broadcast_rate({topic, _source_token, _payload} = data) when topic in @topics do
    Phoenix.PubSub.broadcast(Logflare.PubSub, topic, data)
  end

  @doc """
  The number of partitions for a paritioned child.
  """
  @spec partitions() :: integer()
  def partitions(), do: @partitions

  @doc """
  Partitions a topic for a source_token.
  """
  def partitioned_topic(topic, source_token) do
    topic <> (:erlang.phash2(source_token, partitions()) |> Integer.to_string())
  end
end
