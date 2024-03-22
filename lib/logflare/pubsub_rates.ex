defmodule Logflare.PubSubRates do
  @moduledoc false
  use Supervisor

  alias Logflare.PubSubRates
  alias Phoenix.PubSub
  @topics [:buffers, :rates, :inserts]

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      PubSubRates.Cache,
      PubSubRates.Rates,
      PubSubRates.Buffers,
      PubSubRates.Inserts
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Subscribe to all rate topics

  ### Examples

    iex> subscribe(:all)
    iex> subscribe(:buffers)
    iex> subscribe(:inserts)
    iex> subscribe(:rates)
  """
  @spec subscribe(atom()) :: :ok
  def subscribe(:all), do: subscribe(@topics)
  def subscribe(topics) when is_list(topics), do: Enum.map(topics, &subscribe/1)

  def subscribe(topic) when topic in @topics do
    pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]

    for shard <- 1..pool_size do
      PubSub.subscribe(Logflare.PubSub, "#{topic}:shard-#{shard}")
    end

    :ok
  end

  @doc """
  Global sharded broadcast for a rate-specific message.

  """
  @spec global_broadcast_rate({atom(), atom(), term()}) :: :ok
  def global_broadcast_rate({msg, source_token, _payload} = data) when msg in @topics do
    pool_size = Application.get_env(:logflare, Logflare.PubSub)[:pool_size]
    shard = :erlang.phash2(source_token, pool_size)

    Phoenix.PubSub.broadcast(
      Logflare.PubSub,
      "#{msg}:shard-#{shard}",
      data
    )
  end
end
