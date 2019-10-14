defmodule Logflare.Tracker do
  @behaviour Phoenix.Tracker

  require Logger

  @timeout 1_000

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

  def update(tracker_name, pid, topic, key, meta) do
    task = Phoenix.Tracker.update(tracker_name, pid, topic, key, meta)

    case Task.yield(task, @timeout) || Task.shutdown(task) do
      {:ok, result} ->
        result

      nil ->
        Logger.warn("Tracker timed out in #{@timeout}ms")
        nil
    end
  end

  def track(tracker_name, pid, topic, key, meta) do
    task = Phoenix.Tracker.track(tracker_name, pid, topic, key, meta)

    case Task.yield(task, @timeout) || Task.shutdown(task) do
      {:ok, result} ->
        result

      nil ->
        Logger.warn("Tracker timed out in #{@timeout}ms")
        nil
    end
  end

  def dirty_list(tracker_name, topic) do
    pool_size = Application.get_env(:logflare, __MODULE__)[:pool_size]

    tracker_name
    |> Phoenix.Tracker.Shard.name_for_topic(topic, pool_size)
    |> Phoenix.Tracker.Shard.dirty_list(topic)
  end
end
