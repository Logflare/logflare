defmodule Logflare.Tracker do
  @behaviour Phoenix.Tracker

  def start_link(opts) do
    opts = Keyword.merge([name: __MODULE__], opts)
    Phoenix.Tracker.start_link(__MODULE__, opts, opts)
  end

  def init(opts) do
    server = Keyword.fetch!(opts, :pubsub_server)
    {:ok, %{pubsub_server: server, node_name: Phoenix.PubSub.node_name(server)}}
  end

  def handle_diff(diff, state) do
    {:ok, state}
  end

  def dirty_list(tracker_name, topic) do
    pool_size = Application.get_env(:logflare, __MODULE__)[:pool_size]

    tracker_name
    |> Phoenix.Tracker.Shard.name_for_topic(topic, pool_size)
    |> Phoenix.Tracker.Shard.list(topic)
  end
end
