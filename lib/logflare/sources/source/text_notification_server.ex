defmodule Logflare.Sources.Source.TextNotificationServer do
  @moduledoc false
  use GenServer

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.TeamUsers
  alias Logflare.Users
  alias LogflareWeb.Endpoint
  alias LogflareWeb.Router.Helpers, as: Routes

  require Logger

  @twilio_phone "+16026006731"

  def start_link(args) do
    source = Keyword.get(args, :source)
    GenServer.start_link(__MODULE__, args, name: Backends.via_source(source, __MODULE__))
  end

  def init(args) do
    source = Keyword.get(args, :source)
    check_rate(source.notifications_every)

    {:ok, current_inserts} = Counters.get_inserts(source.token)

    {:ok,
     %{
       source_token: source.token,
       notifications_every: source.notifications_every,
       inserts_since_boot: current_inserts,
       plan: args[:plan]
     }}
  end

  def handle_info(:check_rate, %{plan: %_{name: "Free"}} = state), do: {:noreply, state}

  def handle_info(:check_rate, state) do
    {:ok, current_inserts} = Counters.get_inserts(state.source_token)
    rate = current_inserts - state.inserts_since_boot

    if rate > 0 do
      check_rate(state.notifications_every)

      source = Sources.Cache.get_by_id(state.source_token)

      if source.notifications.user_text_notifications do
        send_text_notifications(source, rate)
      end

      {:noreply, %{state | inserts_since_boot: current_inserts}}
    else
      check_rate(state.notifications_every)
      {:noreply, state}
    end
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end

  defp send_text_notifications(source, rate) do
    user = Users.Cache.get_by(id: source.user_id)
    source_link = Routes.source_url(Endpoint, :show, source.id)
    body = "#{source.name} has #{rate} new event(s). See: #{source_link} "

    ExTwilio.Message.create(to: user.phone, from: @twilio_phone, body: body)

    source.notifications
    |> Map.get(:team_user_ids_for_sms, [])
    |> Enum.each(fn x ->
      team_user = TeamUsers.Cache.get_team_user(x)

      if team_user do
        ExTwilio.Message.create(to: team_user.phone, from: @twilio_phone, body: body)
      end
    end)
  end
end
