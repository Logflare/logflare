defmodule Logflare.AccountEmail do
  @moduledoc false
  import Swoosh.Email

  require Logger

  alias LogflareWeb.Router.Helpers, as: Routes
  alias LogflareWeb.Endpoint
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.{Auth, User, Source, TeamUsers.TeamUser}

  def welcome(user) do
    new()
    |> to(user.email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Welcome to Logflare!")
    |> text_body("""
    Hey stranger!

    Thanks for checking out Logflare! Please Let us know if you have any issues. A few things to note...

    You'll only get account related emails from us. If you want to hear more, please subscribe to product updates: #{Routes.user_url(Endpoint, :edit) <> "#contact-information"}

    Need to cancel your account? No need to contact us. You can always delete your account here: #{Routes.user_url(Endpoint, :edit) <> "#delete-your-account"}

    It's a good idea to take some time to learn about Logflare. Logflare is simple on the surface but enables some powerful things like:

    1) Keeping data forever in your own BigQuery tables. Optionally Bring Your Own BigQuery tables and keep your data around for ultimately half the price of S3: https://logflare.app/guides/bigquery-setup

    2) Enable realtime Google Data Studio reporting when your data pipeline is built with Logflare: https://www.loom.com/share/4e916a4fba9c4aada7b8c10de9f573fb

    3) With the same simple yet powerful query language you can quickly search your logs and build alerts: https://www.loom.com/share/961f3392e20941929ee24393bf01b43c

    And so much more. Check out our documentation site for more information: https://docs.logflare.app

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
    signature = Auth.gen_email_token(user.email_preferred)
    source_link = Routes.source_url(Endpoint, :show, source.id)
    unsubscribe_link = Routes.unsubscribe_url(Endpoint, :unsubscribe, source.id, signature)

    notification_email(
      user.email_preferred,
      rate,
      source.name,
      source_link,
      unsubscribe_link
    )
  end

  def source_notification(%TeamUser{} = user, rate, source) do
    signature = Auth.gen_email_token(user.email_preferred)
    source_link = Routes.source_url(Endpoint, :show, source.id)

    unsubscribe_link =
      Routes.unsubscribe_url(Endpoint, :unsubscribe_team_user, source.id, signature)

    notification_email(
      user.email_preferred,
      rate,
      source.name,
      source_link,
      unsubscribe_link
    )
  end

  def source_notification_for_others(email, rate, source) do
    signature = Auth.gen_email_token(email)
    source_link = Routes.source_url(Endpoint, :show, source.id)

    unsubscribe_link =
      Routes.unsubscribe_url(Endpoint, :unsubscribe_stranger, source.id, signature)

    notification_email(email, rate, source.name, source_link, unsubscribe_link)
  end

  def backend_disconnected(user, reason) do
    account_edit_link = Routes.user_url(Endpoint, :edit) <> "#bigquery-backend"

    new()
    |> to(user.email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("Logflare BigQuery Backend Disabled")
    |> text_body("""
    We had some issues inserting log events into your backend.

    The reason:
    #{reason}

    If this continues please reply to this email and let us know!

    Setup your backend again:
    #{account_edit_link}
    """)
  end

  def schema_updated(
        %User{email_preferred: email},
        %Source{} = source,
        new_schema,
        old_schema
      ),
      do: schema_updated(email, source, new_schema, old_schema, type: :user)

  def schema_updated(
        %TeamUser{email_preferred: email},
        %Source{} = source,
        new_schema,
        old_schema
      ),
      do: schema_updated(email, source, new_schema, old_schema, type: :team_user)

  def schema_updated(
        email,
        %Source{} = source,
        new_schema,
        old_schema,
        opts
      )
      when is_binary(email) do
    [type: type] = opts
    source_link = Routes.source_url(Endpoint, :show, source.id)

    manage_schema_notifications_link =
      Routes.source_url(Endpoint, :edit, source.id) <> "#schema-change-alerts"

    signature = Auth.gen_email_token(email)

    unsubscribe_link =
      Routes.unsubscribe_url(Endpoint, :unsubscribe, source.id, signature, type: type)

    formatted_new = BqSchema.format_bq_schema(new_schema, type: :text)
    formatted_old = BqSchema.format_bq_schema(old_schema, type: :text)

    diff = diff_schema(schema_to_list(formatted_new), schema_to_list(formatted_old))

    if diff == [] do
      # Something generates BOOL and something else generates BOOLEAN which causes this
      Logger.error("Schema update email send with no new fields.",
        source_id: source.token,
        account_email: %{new_schema: inspect(new_schema), old_schema: inspect(old_schema)}
      )
    end

    new =
      formatted_new
      |> schema_to_list()
      |> concat_and_join()

    new_fields =
      diff
      |> concat_and_join()

    part_one = """
    Your source schema has been updated. Based on the incoming payload we've detected some new fields.

    Schema changes are locked for the next 10 seconds. You're allowed to update the schema 6 times per minute per source.

    Note: if you've recently cleared the source cache you may see these emails when the schema cache is rebuilt.

    Source:
    #{source_link}

    New fields:
    #{new_fields}
    """

    part_two = """
    Full schema:
    #{new}
    """

    unsuscribe_part = """
    Manage schema change alerts:
    #{manage_schema_notifications_link}

    Unsubscribe:
    #{unsubscribe_link}
    """

    new()
    |> to(email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("New Fields Found for Logflare Source #{source.name}")
    |> text_body(part_one <> part_two <> unsuscribe_part)
  end

  defp notification_email(email, rate, source_name, source_link, unsubscribe_link) do
    new()
    |> to(email)
    |> from({"Logflare", "support@logflare.app"})
    |> subject("#{rate} New Logflare Event(s) for #{source_name}")
    |> text_body("""
    Source #{source_name} has #{rate} new event(s).

    Source:
    #{source_link}

    Unsubscribe:
    #{unsubscribe_link}
    """)
  end

  defp schema_to_list(schema) do
    for {field, type} <- schema do
      [field, ": ", type, "\n"]
    end
  end

  defp diff_schema(new_schema_list, old_schema_list) do
    new_schema_list -- old_schema_list
  end

  defp concat_and_join(list) do
    list
    |> Enum.concat()
    |> Enum.join()
  end
end
