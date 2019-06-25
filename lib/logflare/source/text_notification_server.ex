defmodule Logflare.Source.TextNotificationServer do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.{Sources, Users}
  alias Logflare.Sources.Counters
  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias Logflare.Source.RecentLogsServer, as: RLS

  @twilio_phone "+16026006731"

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: name(source_id))
  end

  def init(rls) do
    check_rate(rls.notifications_every)
    Process.flag(:trap_exit, true)

    {:ok, current_inserts} = Counters.get_inserts(rls.source_id)

    Logger.info("Table texter started: #{rls.source_id}")

    {:ok, %{rls | inserts_since_boot: current_inserts}}
  end

  def handle_info(:check_rate, rls) do
    {:ok, current_inserts} = Counters.get_inserts(rls.source_id)
    rate = current_inserts - rls.inserts_since_boot

    case rate > 0 do
      true ->
        check_rate(rls.notifications_every)

        source = Sources.Cache.get_by_id(rls.source_id)
        user = Users.Cache.get_by_id(source.user_id)
        source_link = build_host() <> Routes.source_path(Endpoint, :show, rls.source_id)

        {target_number, body} =
          {user.phone, "#{source.name} has #{rate} new event(s). See: #{source_link} "}

        if source.user_text_notifications == true do
          Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
            ExTwilio.Message.create(to: target_number, from: @twilio_phone, body: body)
          end)
        end

        {:noreply, %{rls | inserts_since_boot: current_inserts}}

      false ->
        check_rate(rls.notifications_every)
        {:noreply, rls}
    end
  end

  def terminate(reason, _state) do
    # Do Shutdown Stuff
    Logger.info("Going Down: #{__MODULE__}")
    reason
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-texter")
  end

  defp build_host() do
    host_info = LogflareWeb.Endpoint.struct_url()
    host_info.scheme <> "://" <> host_info.host
  end
end
