defmodule Logflare.Admin do
  require Logger

  alias Logflare.{Repo, Source}

  def copy_old_notification_prefs(%Source{} = source) do
    params = %{
      "notifications" => %{
        "other_email_notifications" => source.other_email_notifications,
        "user_email_notifications" => source.user_email_notifications,
        "user_text_notifications" => source.user_text_notifications
      }
    }

    Source.changeset(source, params)
    |> Repo.update()

    Logger.info("Source notifications updated!", source_id: source.token)
  end

  def copy_old_notification_prefs(sources) when is_list(sources) do
    Enum.each(sources, fn source ->
      copy_old_notification_prefs(source)
    end)
  end
end
