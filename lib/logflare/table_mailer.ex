defmodule Logflare.TableMailer do
  use GenServer

  require Logger

  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.User
  alias Logflare.AccountEmail
  alias Logflare.Mailer
  alias Logflare.SourceRateCounter

  @check_rate_every 1_000

  def start_link(website_table) do
    GenServer.start_link(
      __MODULE__,
      %{
        source: website_table,
        events: []
      },
      name: name(website_table)
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
        {:ok, user, source} = get_user_and_source(state.source)

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

  defp get_user_and_source(website_table) do
    source = Repo.get_by(Source, token: Atom.to_string(website_table))
    user = Repo.get(User, source.user_id)
    {:ok, user, source}
  end

  defp check_rate() do
    Process.send_after(self(), :check_rate, @check_rate_every)
  end

  defp name(website_table) do
    String.to_atom("#{website_table}" <> "-mailer")
  end
end
