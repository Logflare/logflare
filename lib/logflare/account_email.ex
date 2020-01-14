defmodule Logflare.AccountEmail do
  import Swoosh.Email

  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias Logflare.Auth

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

  def source_notification(user, rate, source) do
    signature = Auth.gen_token(user.email_preferred)
    source_link = Routes.source_url(Endpoint, :show, source.id)
    unsubscribe_link = Routes.unsubscribe_url(Endpoint, :unsubscribe, source.id, signature)

    new()
    |> to(user.email_preferred)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("#{rate} New Logflare Event(s) for #{source.name}!")
    |> text_body(
      "Your source #{source.name} has #{rate} new event(s).\n\nSee them here: #{source_link}\n\nUnsubscribe: #{
        unsubscribe_link
      }"
    )
  end

  def source_notification_for_others(email, rate, source) do
    signature = Auth.gen_token(email)
    source_link = Routes.source_url(Endpoint, :show, source.id)

    unsubscribe_link =
      Routes.unsubscribe_url(Endpoint, :unsubscribe_stranger, source.id, signature)

    signup_link = Routes.auth_url(Endpoint, :login)

    new()
    |> to(email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("#{rate} New Logflare Event(s) for #{source.name}!")
    |> text_body(
      "Source #{source.name} has #{rate} new event(s).\n\nSee them here: #{source_link}\n\nSign up for Logflare: #{
        signup_link
      }\n\nUnsubscribe: #{unsubscribe_link}"
    )
  end
end
