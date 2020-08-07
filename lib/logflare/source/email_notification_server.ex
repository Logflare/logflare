defmodule Logflare.Source.EmailNotificationServer do
  use GenServer

  require Logger

  alias Logflare.{Sources, Users, TeamUsers}
  alias Logflare.Sources.Counters
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.Source.RecentLogsServer, as: RLS

  def start_link(%RLS{source_id: source_id} = rls) when is_atom(source_id) do
    GenServer.start_link(__MODULE__, rls, name: name(source_id))
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

    case rate > 0 do
      true ->
        check_rate(rls.notifications_every)

        source = Sources.Cache.get_by_id(rls.source_id)
        user = Users.Cache.get_by(id: source.user_id)

        if source.notifications.user_email_notifications == true do
          Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
            AccountEmail.source_notification(user, rate, source) |> Mailer.deliver()
          end)
        end

        stranger_emails = source.notifications.other_email_notifications

        if stranger_emails do
          other_emails = String.split(stranger_emails, ",")

          for email <- other_emails do
            Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
              AccountEmail.source_notification_for_others(String.trim(email), rate, source)
              |> Mailer.deliver()
            end)
          end
        end

        if source.notifications.team_user_ids_for_email do
          Enum.each(source.notifications.team_user_ids_for_email, fn x ->
            team_user = TeamUsers.get_team_user(x)

            if team_user do
              Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
                AccountEmail.source_notification(team_user, rate, source) |> Mailer.deliver()
              end)
            end
          end)
        end

        {:noreply, %{rls | inserts_since_boot: current_inserts}}

      false ->
        check_rate(rls.notifications_every)
        {:noreply, rls}
    end
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
    String.to_atom("#{source_id}" <> "-mailer")
  end
end
