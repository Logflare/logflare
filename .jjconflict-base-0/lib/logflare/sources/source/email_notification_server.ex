defmodule Logflare.Sources.Source.EmailNotificationServer do
  @moduledoc false
  use GenServer

  alias Logflare.AccountEmail
  alias Logflare.Backends
  alias Logflare.Mailer
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.TeamUsers
  alias Logflare.Users

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

    check_rate(state.notifications_every)

    if rate > 0 do
      source = Sources.Cache.get_by_id(state.source_token)
      deliver_notifications(source, rate)
      {:noreply, %{state | inserts_since_boot: current_inserts}}
    else
      {:noreply, state}
    end
  end

  defp check_rate(notifications_every) do
    Process.send_after(self(), :check_rate, notifications_every)
  end

  defp deliver_notifications(source, rate) do
    notify_user(source, rate)
    notify_other_emails(source, rate)
    notify_team_users(source, rate)
  end

  defp notify_user(source, rate) do
    if source.notifications.user_email_notifications do
      user = Users.Cache.get(source.user_id)
      AccountEmail.source_notification(user, rate, source) |> Mailer.deliver()
    end
  end

  defp notify_other_emails(source, rate) do
    if stranger_emails = source.notifications.other_email_notifications do
      stranger_emails
      |> String.split(",")
      |> Enum.each(fn email ->
        email
        |> String.trim()
        |> AccountEmail.source_notification_for_others(rate, source)
        |> Mailer.deliver()
      end)
    end
  end

  defp notify_team_users(source, rate) do
    if source.notifications.team_user_ids_for_email do
      send_email_notification(source, rate)
    end
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
