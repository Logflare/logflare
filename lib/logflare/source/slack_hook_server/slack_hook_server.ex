defmodule Logflare.Source.SlackHookServer do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Backends

  def start_link(args) do
    source = Keyword.get(args, :source)
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__))
  end

  def test_post(source) do
    recent_events = Backends.list_recent_logs(source)

    __MODULE__.Client.new()
    |> __MODULE__.Client.post(source, source.metrics.rate, recent_events)
  end

  def init(args) do
    source = Keyword.get(args, :source)
    check_rate(source.notifications_every)

    {:ok, current_inserts} = Counters.get_inserts(source.token)

    {:ok,
     %{
       source_id: source.id,
       source_token: source.token,
       notifications_every: source.notifications_every,
       inserts_since_boot: current_inserts
     }}
  end

  def handle_info(:check_rate, state) do
    {:ok, current_inserts} = Counters.get_inserts(state.source_token)
    rate = current_inserts - state.inserts_since_boot
    source = Sources.Cache.get_by_id(state.source_token)

    case rate > 0 do
      true ->
        if source.slack_hook_url do
          recent_events = Backends.list_recent_logs(source)

          __MODULE__.Client.new()
          |> __MODULE__.Client.post(source, rate, recent_events)
        end

        check_rate(state.notifications_every)
        {:noreply, %{state | inserts_since_boot: current_inserts}}

      false ->
        check_rate(state.notifications_every)
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _pid, :normal}, state) do
    :noop

    {:noreply, state}
  end

  def handle_info({:ssl_closed, _details}, state) do
    # See https://github.com/benoitc/hackney/issues/464
    :noop

    {:noreply, state}
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end
end
