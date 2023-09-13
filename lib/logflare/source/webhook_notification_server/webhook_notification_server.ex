defmodule Logflare.Source.WebhookNotificationServer do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias __MODULE__, as: WNS

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: Source.Supervisor.via(__MODULE__, source_id))
  end

  def test_post(source) do
    recent_events = RLS.list(source.token)
    uri = source.webhook_notification_url

    post(uri, source, 0, recent_events)
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
        if uri = source.webhook_notification_url do
          recent_events = RLS.list(rls.source_id)

          post(uri, source, rate, recent_events)
        end

        check_rate(rls.notifications_every)
        {:noreply, %{rls | inserts_since_boot: current_inserts}, :hibernate}

      false ->
        check_rate(rls.notifications_every)
        {:noreply, rls, :hibernate}
    end
  end

  def handle_info({:ssl_closed, _details}, rls) do
    # See https://github.com/benoitc/hackney/issues/464
    :noop

    {:noreply, rls}
  end

  def handle_info({:EXIT, _pid, :normal}, rls) do
    :noop

    {:noreply, rls}
  end

  def terminate(reason, state) do
    # Do Shutdown Stuff
    Logger.info("Going Down - #{inspect(reason)} - #{__MODULE__}", %{source_id: state.source_id})
    reason
  end

  defp post(uri, source, rate, recent_events) do
    case URI.parse(uri) do
      %URI{host: "discordapp.com"} ->
        WNS.DiscordClient.new()
        |> WNS.DiscordClient.post(source, rate, recent_events)

      %URI{host: "ptb.discord.com"} = _uri ->
        WNS.DiscordClient.new()
        |> WNS.DiscordClient.post(source, rate, recent_events)

      %URI{host: "discord.com"} = _uri ->
        WNS.DiscordClient.new()
        |> WNS.DiscordClient.post(source, rate, recent_events)

      %URI{host: nil} ->
        {:error, :bad_uri}

      %URI{} ->
        WNS.Client.new()
        |> WNS.Client.post(source, rate, recent_events)
    end
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end
end
