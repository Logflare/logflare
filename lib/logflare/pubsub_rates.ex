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
    PubSub.subscribe(Logflare.PubSub, "#{topic}")
  end

  @doc """
  Global sharded broadcast for a rate-specific message.
  """
  @spec global_broadcast_rate({atom(), non_neg_integer(), nil | non_neg_integer(), term()}) :: :ok
  @spec global_broadcast_rate({atom(), atom(), term()}) :: :ok
  def global_broadcast_rate({msg, source_id, _backend_id, _payload} = data)
      when msg in @topics and is_integer(source_id) do
    Phoenix.PubSub.broadcast(Logflare.PubSub, "#{msg}", data)
  end

  def global_broadcast_rate({msg, _source_token, _payload} = data) when msg in @topics do
    Phoenix.PubSub.broadcast(Logflare.PubSub, "#{msg}", data)
  end
end
