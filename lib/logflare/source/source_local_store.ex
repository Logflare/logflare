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
  def start_link(%RLS{} = args, opts \\ []) do
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

    {:ok, total_log_count} = ClusterStore.get_total_log_count(source_id)

    {:ok, prev_max} = ClusterStore.get_max_rate(source_id)
    {:ok, buffer} = ClusterStore.get_buffer_count(source_id)
    {:ok, avg} = ClusterStore.get_avg_rate(source_id)

    {:ok, last_source_rate} = ClusterStore.get_source_last_rate(source.token, period: :second)

    max = Enum.max([prev_max, last_source_rate])

    rates_payload = %{
      last_rate: last_source_rate || 0,
      rate: last_source_rate || 0,
      average_rate: round(avg),
      max_rate: max || 0,
      source_token: source.token
    }

    buffer_payload = %{source_token: state.source_id, buffer: buffer}
    log_count_payload = %{source_token: state.source_id, log_count: total_log_count}

    Source.ChannelTopics.broadcast_rates(rates_payload)
    Source.ChannelTopics.broadcast_buffer(buffer_payload)
    Source.ChannelTopics.broadcast_log_count(log_count_payload)

    if max > prev_max do
      ClusterStore.set_max_rate(source_id, max)
    end

    {:ok, last_source_minute_rate} =
      ClusterStore.get_source_last_rate(source.token, period: :minute)

    {:ok, last_user_minute_rate} =
      ClusterStore.get_user_last_rate(source.user.id, period: :minute)

    parse_rate = fn maybe_num ->
      cond do
        is_nil(maybe_num) -> 0
        is_integer(maybe_num) -> maybe_num
        is_binary(maybe_num) -> String.to_integer(maybe_num)
      end
    end

    Users.API.Cache.put_user_rate(source.user, parse_rate.(last_user_minute_rate))
    Users.API.Cache.put_source_rate(source, parse_rate.(last_source_minute_rate))

    {:noreply, state}
  end

  def tick() do
    Process.send_after(self(), :tick, @tick_interval)
  end
end
