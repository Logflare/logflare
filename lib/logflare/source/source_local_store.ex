defmodule Logflare.Source.LocalStore do
  @moduledoc """
   Handles Redis source counter caches
  """
  alias Logflare.Source
  alias Logflare.{Sources, Users}
  alias Logflare.Sources.ClusterStore
  alias Logflare.Source.RecentLogsServer, as: RLS
  use GenServer

  @tick_interval 1_000

  # def start_link(%{source: %Source{} = source} = args, opts) do
  def start_link(%RLS{source_id: source_id} = args, opts \\ []) do
    GenServer.start_link(__MODULE__, args, opts)
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  def init(args) do
    tick()

    {:ok, args}
  end

  def handle_info(:tick, %{source_id: source_id} = state) do
    tick()

    source = Sources.Cache.get_by_id(source_id)

    {:ok, source_counters_sec} = ClusterStore.get_source_log_counts(source)
    {:ok, user_log_counts} = ClusterStore.get_user_log_counts(source.user.id)
    {:ok, total_log_count} = ClusterStore.get_total_log_count(source_id)

    unless Enum.empty?(source_counters_sec) do
      {:ok, prev_max} = ClusterStore.get_max_rate(source_id)
      {:ok, buffer} = ClusterStore.get_buffer_count(source_id)
      prev_max = prev_max || 0
      max = Enum.max([prev_max | source_counters_sec])
      avg = Enum.sum(source_counters_sec) / Enum.count(source_counters_sec)
      last = hd(source_counters_sec)

      rates_payload = %{
        last_rate: last || 0,
        rate: last || 0,
        average_rate: round(avg),
        max_rate: max || 0,
        source_token: source.token
      }

      buffer_payload = %{source_token: state.source_id, buffer: buffer}
      log_count_payload = %{source_token: state.source_id, log_count: total_log_count}

      Source.ChannelTopics.broadcast_rates(rates_payload)
      Source.ChannelTopics.broadcast_buffer(buffer_payload)
      Source.ChannelTopics.broadcast_log_count(log_count_payload)

      ClusterStore.set_max_rate(source_id, max)
      ClusterStore.set_avg_rate(source_id, avg)
      ClusterStore.set_last_rate(source_id, last)
    end

    sum_log_counts = fn counts -> counts |> Enum.slice(0..60) |> Enum.sum() end

    source_rate = sum_log_counts.(source_counters_sec)
    user_rate = sum_log_counts.(user_log_counts)

    Users.API.Cache.put_user_rate(source.user, user_rate)
    Users.API.Cache.put_source_rate(source, source_rate)

    {:noreply, state}
  end

  def tick() do
    Process.send_after(self(), :tick, @tick_interval)
  end
end
