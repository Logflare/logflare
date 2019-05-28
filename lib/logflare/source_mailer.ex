defmodule Logflare.SourceMailer do
  use GenServer

  require Logger

  alias Logflare.{Sources, Users}
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.SourceRateCounter

  @check_rate_every 1_000

  def start_link(source_id) do
    GenServer.start_link(
      __MODULE__,
      %{
        source: source_id,
        events: []
      },
      name: name(source_id)
    )
  end

  def init(state) do
    Logger.info("Table mailer started: #{state.source}")
    check_rate()
    {:ok, state}
  end

  def handle_info(:check_rate, state) do
    rate = SourceRateCounter.get_rate(state.source)

    case rate > 0 do
      true ->
        source = Sources.Cache.get_by_id(state.source)
        user = Users.Cache.get_by_id(source.user_id)

        if source.user_email_notifications == true do
          Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
            AccountEmail.source_notification(user, rate, source) |> Mailer.deliver()
          end)
        end

        stranger_emails = source.other_email_notifications

        if is_nil(stranger_emails) == false do
          other_emails = String.split(stranger_emails, ",")

          for email <- other_emails do
            Task.Supervisor.start_child(Logflare.TaskSupervisor, fn ->
              AccountEmail.source_notification_for_others(String.trim(email), rate, source)
              |> Mailer.deliver()
            end)
          end
        end

        check_rate()
        {:noreply, state}

      false ->
        check_rate()
        {:noreply, state}
    end
  end

  defp check_rate() do
    Process.send_after(self(), :check_rate, @check_rate_every)
  end

  defp name(source_id) do
    String.to_atom("#{source_id}" <> "-mailer")
  end
end
