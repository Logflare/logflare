defmodule Logflare.Source.LocalStore do
  @moduledoc """
   Handles Redis source counter caches
  """
  alias Logflare.Source
  alias Logflare.{Sources, Users}
  alias Logflare.Sources.ClusterStore
  alias Logflare.Source.RecentLogsServer, as: RLS
  use GenServer

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

    {:ok, sec_counters} = ClusterStore.get_source_log_counts(source)

    unless Enum.empty?(sec_counters) do
      {:ok, prev_max} = ClusterStore.get_max_rate(source_id)
      prev_max = prev_max || 0
      max = Enum.max([prev_max | sec_counters])
      avg = Enum.sum(sec_counters) / Enum.count(sec_counters)
      last = hd(sec_counters)

      rates = %{
        last_rate: last,
        rate: last,
        average_rate: round(avg),
        max_rate: max,
        source_token: source.token
      }

      Source.ChannelTopics.broadcast_rates(rates)

      ClusterStore.set_max_rate(source_id, max)
      ClusterStore.set_avg_rate(source_id, avg)
      ClusterStore.set_last_rate(source_id, last)
    end

    sum_log_counts = fn counts -> counts |> Enum.slice(0..60) |> Enum.sum() end
    source_rate = sum_log_counts.(sec_counters)

    {:ok, user_log_counts} = ClusterStore.get_user_log_counts(source.user.id)

    user_rate = sum_log_counts.(user_log_counts)

    Users.API.Cache.put_user_rate(source.user, user_rate)
    Users.API.Cache.put_source_rate(source, source_rate)

    {:noreply, state}
  end

  def tick() do
    Process.send_after(self(), :tick, 100)
  end
end
