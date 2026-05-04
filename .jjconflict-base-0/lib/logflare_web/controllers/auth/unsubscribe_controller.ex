defmodule LogflareWeb.Auth.UnsubscribeController do
  use LogflareWeb, :controller

  alias Logflare.Auth
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.TeamUsers

  @max_age 86_400

  def unsubscribe(conn, %{"id" => source_id, "token" => token, "type" => "team_user"}) do
    case Auth.verify_email_token(token, @max_age) do
      {:ok, email} ->
        team_user = TeamUsers.get_team_user_by(email: email)
        source = Sources.get(String.to_integer(source_id))

        ids =
          Enum.filter(source.notifications.team_user_ids_for_schema_updates, fn x ->
            x != to_string(team_user.id)
          end)

        changeset =
          Source.update_by_user_changeset(source, %{
            notifications: %{team_user_ids_for_schema_updates: ids}
          })

        update_source(conn, changeset)

      {:error, :expired} ->
        error_and_redirect(conn, "That link is expired!")

      {:error, :invalid} ->
        error_and_redirect(conn, "Bad link!")
    end
  end

  def unsubscribe(conn, %{"id" => source_id, "token" => token, "type" => "user"}) do
    case Auth.verify_email_token(token, @max_age) do
      {:ok, _email} ->
        # We don't have the source in the assigns because we don't require auth to unsubscribe
        source = Sources.get(String.to_integer(source_id))

        changeset =
          Source.update_by_user_changeset(source, %{
            notifications: %{user_schema_update_notifications: false}
          })

        update_source(conn, changeset)

      {:error, :expired} ->
        error_and_redirect(conn, "That link is expired!")

      {:error, :invalid} ->
        error_and_redirect(conn, "Bad link!")
    end
  end

  def unsubscribe(conn, %{"id" => source_id, "token" => token}) do
    case Auth.verify_email_token(token, @max_age) do
      {:ok, _email} ->
        # We don't have the source in the assigns because we don't require auth to unsubscribe
        source = Sources.get(String.to_integer(source_id))

        changeset =
          Source.update_by_user_changeset(source, %{
            notifications: %{user_email_notifications: false}
          })

        update_source(conn, changeset)

      {:error, :expired} ->
        error_and_redirect(conn, "That link is expired!")

      {:error, :invalid} ->
        error_and_redirect(conn, "Bad link!")
    end
  end

  def unsubscribe_stranger(conn, %{"id" => source_id, "token" => token}) do
    case Auth.verify_email_token(token, @max_age) do
      {:ok, email} ->
        # We don't have the source in the assigns because we don't require auth to unsubscribe
        source = Sources.get(String.to_integer(source_id))

        changeset =
          Source.update_by_user_changeset(source, %{
            notifications: %{
              other_email_notifications:
                filter_email(email, source.notifications.other_email_notifications)
            }
          })

        update_source(conn, changeset)

      {:error, :expired} ->
        error_and_redirect(conn, "That link is expired!")

      {:error, :invalid} ->
        error_and_redirect(conn, "Bad link!")
    end
  end

  def unsubscribe_team_user(conn, %{"id" => source_id, "token" => token}) do
    case Auth.verify_email_token(token, @max_age) do
      {:ok, email} ->
        team_user = TeamUsers.get_team_user_by(email: email)
        source = Sources.get(String.to_integer(source_id))

        team_user_ids_for_email =
          Enum.filter(source.notifications.team_user_ids_for_email, fn x ->
            x != to_string(team_user.id)
          end)

        changeset =
          Source.update_by_user_changeset(source, %{
            notifications: %{team_user_ids_for_email: team_user_ids_for_email}
          })

        update_source(conn, changeset)

      {:error, :expired} ->
        error_and_redirect(conn, "That link is expired!")

      {:error, :invalid} ->
        error_and_redirect(conn, "Bad link!")
    end
  end

  defp update_source(conn, changeset) do
    case Sources.update_source(changeset) do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Unsubscribed!")
        |> redirect(to: ~p"/auth/login")

      {:error, _changeset} ->
        error_and_redirect(
          conn,
          "Something went wrong! Please contact support if this continues."
        )
    end
  end

  defp error_and_redirect(conn, message) do
    conn
    |> put_flash(:error, message)
    |> redirect(to: ~p"/auth/login")
  end

  defp filter_email(_email, other_emails) when is_nil(other_emails), do: nil

  defp filter_email(email, other_emails) when is_binary(other_emails) do
    String.split(other_emails, ",")
    |> Enum.map(fn e -> String.trim(e) end)
    |> Enum.filter(fn e -> e != email end)
    |> Enum.join(", ")
  end
end
