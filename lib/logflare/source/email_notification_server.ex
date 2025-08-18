defmodule Logflare.Source.EmailNotificationServer do
  @moduledoc false
  use GenServer

  require Logger

  alias Logflare.{Sources, Users, TeamUsers}
  alias Logflare.Sources.Counters
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Backends

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
       notifications_every: source.notifications_every,
       source_token: source.token,
       inserts_since_boot: current_inserts
     }}
  end

  def handle_info(:check_rate, state) do
    {:ok, current_inserts} = Counters.get_inserts(state.source_token)
    rate = current_inserts - state.inserts_since_boot

    if rate > 0 do
      check_rate(state.notifications_every)

      source = Sources.Cache.get_by_id(state.source_token)
      user = Users.Cache.get(source.user_id)

      if source.notifications.user_email_notifications do
        AccountEmail.source_notification(user, rate, source) |> Mailer.deliver()
      end

      stranger_emails = source.notifications.other_email_notifications

      if stranger_emails do
        other_emails = String.split(stranger_emails, ",")

        for email <- other_emails do
          AccountEmail.source_notification_for_others(String.trim(email), rate, source)
          |> Mailer.deliver()
        end
      end

      if source.notifications.team_user_ids_for_email do
        send_email_notification(source, rate)
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

  defp send_email_notification(source, rate) do
    Enum.each(source.notifications.team_user_ids_for_email, fn x ->
      team_user = TeamUsers.Cache.get_team_user(x)

      if team_user do
        team_user |> AccountEmail.source_notification(rate, source) |> Mailer.deliver()
      end
    end)
  end
end
