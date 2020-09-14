defmodule Logflare.AccountEmail do
  import Swoosh.Email

  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias Logflare.{Auth, User, TeamUsers.TeamUser}

  def welcome(user) do
    new()
    |> to(user.email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Welcome to Logflare!")
    |> text_body("""
    Hey stranger!

    Thanks for checking out Logflare! Please Let us know if you have any issues. A few things to note...

    You'll only get account related emails from us. If you want to hear more, please subscribe to product updates: #{
      Routes.user_url(Endpoint, :edit) <> "#contact-information"
    }

    Need to cancel your account? No need to contact us. You can always delete your account here: #{
      Routes.user_url(Endpoint, :edit) <> "#delete-your-account"
    }

    It's a good idea to take some time to learn about Logflare. Logflare is simple on the surface but enables some powerful things like:

    1) Keeping data forever in your own BigQuery tables. Optionally Bring Your Own BigQuery tables and keep your data around for ultimately half the price of S3: https://logflare.app/guides/bigquery-setup

    2) Enable realtime Google Data Studio reporting when your data pipeline is built with Logflare: https://www.loom.com/share/4e916a4fba9c4aada7b8c10de9f573fb

    3) With the same simple yet powerful query language you can quickly search your logs and build alerts: https://www.loom.com/share/961f3392e20941929ee24393bf01b43c

    And so much more. Our guides provide some good info: #{
      Routes.marketing_url(Endpoint, :guides)
    }

    We have a little Loom library if you prefer video: https://loom.com/share/folder/4fd2f989ed1c4e18a3de76773ae9cf3e

    But please, if you have any questions just reach out. If you *think* something is possible with Logflare, it probably is!

    --

    Logger.info("Email sent", %{
      from: %{
        name: "Chase Granberry",
        domain: "logflare.app",
        email: "chase@logflare.app"
      }
    })

    """)
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
    account_edit_link = Routes.user_url(Endpoint, :edit) <> "#bigquery-backend"

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
