defmodule LogflareWeb.TeamUserController do
  use LogflareWeb, :controller
  use Phoenix.HTML

  alias Logflare.TeamUsers

  def edit(%{assigns: %{team_user: team_user}} = conn, _params) do
    changeset = TeamUsers.TeamUser.changeset(team_user, %{})

    render(conn, "edit.html", changeset: changeset, team_user: team_user)
  end

  def update(%{assigns: %{team_user: team_user}} = conn, %{"team_user" => params}) do
    case TeamUsers.update_team_user(team_user, params) do
      {:ok, _team_user} ->
        conn
        |> put_flash(:info, "Profile updated!")
        |> redirect(to: Routes.team_user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html", changeset: changeset, team_user: team_user)
    end
  end

  def delete(%{assigns: %{team_user: team_user}} = conn, _params) do
    case TeamUsers.delete_team_user(team_user) do
      {:ok, _team_user} ->
        conn
        |> configure_session(drop: true)
        |> redirect(to: Routes.auth_path(conn, :login))
        |> put_flash(:info, "Profile deleted!")

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html", changeset: changeset, team_user: team_user)
    end
  end

  def change_team(%{assigns: %{team_user: team_user, user: _user}} = conn, %{
        "user_id" => user_id,
        "team_user_id" => team_user_id
      }) do
    conn
    |> put_session(:user_id, user_id)
    |> put_session(:team_user_id, team_user_id)
    |> put_flash(:info, "Team changed!")
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end

  def change_team(%{assigns: %{team_user: team_user, user: _user}} = conn, %{
        "user_id" => user_id
      }) do
    conn
    |> put_session(:user_id, user_id)
    |> delete_session(:team_user_id)
    |> put_flash(:info, "Team changed!")
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end

  def change_team(%{assigns: %{user: _user}} = conn, %{
        "user_id" => user_id,
        "team_user_id" => team_user_id
      }) do
    conn
    |> put_session(:user_id, user_id)
    |> put_session(:team_user_id, team_user_id)
    |> put_flash(:info, "Team changed!")
    |> redirect(to: Routes.source_path(conn, :dashboard))
  end
end
