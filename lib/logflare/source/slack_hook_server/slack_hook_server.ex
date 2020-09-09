defmodule Logflare.Source.SlackHookServer do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source.Data
  alias __MODULE__, as: SHS

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: name(source_id))
  end

  def test_post(source) do
    recent_events = RLS.list(source.token)

    SHS.Client.new()
    |> SHS.Client.post(source, source.metrics.rate, recent_events)
  end

  def init(rls) do
    check_rate(rls.notifications_every)
    Process.flag(:trap_exit, true)

    {:ok, current_inserts} = Counters.get_inserts(rls.source_id)

    {:ok, %{rls | inserts_since_boot: current_inserts}}
  end

  def handle_info(:check_rate, rls) do
    {:ok, current_inserts} = Counters.get_inserts(rls.source_id)
    rate = current_inserts - rls.inserts_since_boot
    source = Sources.Cache.get_by_id(rls.source_id)

    case rate > 0 do
      true ->
        if source.slack_hook_url do
          recent_events = RLS.list(rls.source_id)

          SHS.Client.new()
          |> SHS.Client.post(source, rate, recent_events)
        end

        check_rate(rls.notifications_every)
        {:noreply, %{rls | inserts_since_boot: current_inserts}}

      false ->
        check_rate(rls.notifications_every)
        {:noreply, rls}
    end
  end

  def handle_info({:EXIT, _pid, :normal}, rls) do
    :noop

    {:noreply, rls}
  end

  def handle_info({:ssl_closed, _details}, rls) do
    # See https://github.com/benoitc/hackney/issues/464
    :noop

    {:noreply, rls}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{source_id: state.source_id})
    reason
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-slackhooks")
  end
end
