defmodule Logflare.AccountEmail do
  import Swoosh.Email

  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias Logflare.{Auth, User, TeamUsers.TeamUser}

  def welcome(user) do
    account_edit_link = Routes.user_url(Endpoint, :edit)

    new()
    |> to(user.email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Welcome to Logflare!")
    |> text_body(
      "Heya!\n\nThanks for checking out Logflare! Let us know if you have any issues :)\n\nYou can always delete your account here: #{
        account_edit_link
      }\n\nSetup Google Data Studio: https://logflare.app/guides/data-studio-setup\nBring your own BigQuery backend: https://logflare.app/guides/bigquery-setup"
    )
  end

  def source_notification(%User{} = user, rate, source) do
    signature = Auth.gen_token(user.email_preferred)
    source_link = Routes.source_url(Endpoint, :show, source.id)
    unsubscribe_link = Routes.unsubscribe_url(Endpoint, :unsubscribe, source.id, signature)

    unsubscribe_email(
      user.email_preferred,
      rate,
      source.name,
      source_link,
      unsubscribe_link
    )
  end

  def source_notification(%TeamUser{} = user, rate, source) do
    signature = Auth.gen_token(user.email_preferred)
    source_link = Routes.source_url(Endpoint, :show, source.id)

    unsubscribe_link =
      Routes.unsubscribe_url(Endpoint, :unsubscribe_team_user, source.id, signature)

    unsubscribe_email(
      user.email_preferred,
      rate,
      source.name,
      source_link,
      unsubscribe_link
    )
  end

  def source_notification_for_others(email, rate, source) do
    signature = Auth.gen_token(email)
    source_link = Routes.source_url(Endpoint, :show, source.id)

    unsubscribe_link =
      Routes.unsubscribe_url(Endpoint, :unsubscribe_stranger, source.id, signature)

    unsubscribe_email(email, rate, source.name, source_link, unsubscribe_link)
  end

  def backend_disconnected(user, reason) do
    account_edit_link = Routes.user_url(Endpoint, :edit) <> "#big-query-preferences"

    new()
    |> to(user.email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Logflare BigQuery Backend Disabled")
    |> text_body(
      "Hey!\n\nWe had some issues inserting log events into your backend. The reason:\n\n#{reason}\n\nIf this continues please reply to this email and let us know!\n\nSetup your backend again: #{
        account_edit_link
      }"
    )
  end

  defp unsubscribe_email(email, rate, source_name, source_link, unsubscribe_link) do
    new()
    |> to(email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("#{rate} New Logflare Event(s) for #{source_name}!")
    |> text_body(
      "Source #{source_name} has #{rate} new event(s).\n\nSee them here: #{source_link}\n\nUnsubscribe: #{
        unsubscribe_link
      }"
    )
  end
end
