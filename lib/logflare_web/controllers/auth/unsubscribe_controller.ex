defmodule LogflareWeb.Auth.UnsubscribeController do
  use LogflareWeb, :controller

  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Auth

  @max_age 86_400

  def unsubscribe(conn, %{"id" => source_id, "token" => token}) do
    source = Repo.get(Source, source_id)
    source_changes = %{user_email_notifications: false}
    changeset = Source.update_by_user_changeset(source, source_changes)

    case Auth.verify_token(token, @max_age) do
      {:ok, _email} ->
        case Repo.update(changeset) do
          {:ok, _source} ->
            conn
            |> put_flash(:info, "Unsubscribed!")
            |> redirect(to: Routes.marketing_path(conn, :index))

          {:error, _changeset} ->
            conn
            |> put_flash(:error, "Something went wrong!")
            |> redirect(to: Routes.marketing_path(conn, :index))
        end

      {:error, :expired} ->
        conn
        |> put_flash(:error, "That link is expired!")
        |> redirect(to: Routes.marketing_path(conn, :index))

      {:error, :invalid} ->
        conn
        |> put_flash(:error, "Bad link!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  def unsubscribe_stranger(conn, %{"id" => source_id, "token" => token}) do
    case Auth.verify_token(token, @max_age) do
      {:ok, email} ->
        source = Repo.get(Source, source_id)

        source_changes = %{
          other_email_notifications: filter_email(email, source.other_email_notifications)
        }

        changeset = Source.update_by_user_changeset(source, source_changes)

        case Repo.update(changeset) do
          {:ok, _source} ->
            conn
            |> put_flash(:info, "Unsubscribed!")
            |> redirect(to: Routes.marketing_path(conn, :index))

          {:error, _changeset} ->
            conn
            |> put_flash(:error, "Something went wrong!")
            |> redirect(to: Routes.marketing_path(conn, :index))
        end

      {:error, :expired} ->
        conn
        |> put_flash(:error, "That link is expired!")
        |> redirect(to: Routes.marketing_path(conn, :index))

      {:error, :invalid} ->
        conn
        |> put_flash(:error, "Bad link!")
        |> redirect(to: Routes.marketing_path(conn, :index))
    end
  end

  defp filter_email(email, other_emails) do
    String.split(other_emails, ",")
    |> Enum.map(fn e -> String.trim(e) end)
    |> Enum.filter(fn e -> e != email end)
    |> Enum.join(", ")
  end
end
