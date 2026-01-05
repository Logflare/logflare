defmodule LogflareWeb.TeamUserController do
  use LogflareWeb, :controller

  plug LogflareWeb.Plugs.AuthMustBeTeamAdmin when action in [:delete, :update_role]

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.TeamUsers
  alias Logflare.Users

  def edit(%{assigns: %{team_user: team_user}} = conn, _params) do
    changeset = TeamUsers.TeamUser.changeset(team_user, %{})

    render(conn, "edit.html", changeset: changeset, team_user: team_user)
  end

  def update(%{assigns: %{team_user: team_user}} = conn, %{"team_user" => params}) do
    case TeamUsers.update_team_user(team_user, params) do
      {:ok, _team_user} ->
        conn
        |> put_flash(:info, "Profile updated!")
        |> redirect(to: ~p"/profile/edit?t=#{team_user.team_id}")

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html", changeset: changeset, team_user: team_user)
    end
  end

  def delete(%{assigns: %{user: user}} = conn, %{"id" => team_user_id} = _params) do
    team_user = TeamUsers.get_team_user!(team_user_id)
    user = Users.preload_team(user)

    if team_user.team_id != user.team.id do
      conn
      |> put_flash(:error, "Not authorized to delete this team member")
      |> redirect(to: Routes.user_path(conn, :edit))
    else
      case TeamUsers.delete_team_user(team_user) do
        {:ok, _team_user} ->
          BigQueryAdaptor.update_iam_policy()
          BigQueryAdaptor.patch_dataset_access(user)

          conn
          |> put_flash(:info, "Member profile deleted!")
          |> redirect(to: Routes.user_path(conn, :edit))

        {:error, _changeset} ->
          conn
          |> put_flash(:error, "Something went wrong!")
          |> redirect(to: Routes.user_path(conn, :edit))
      end
    end
  end

  def delete_self(%{assigns: %{team_user: team_user, user: user}} = conn, _params) do
    case TeamUsers.delete_team_user(team_user) do
      {:ok, _team_user} ->
        BigQueryAdaptor.update_iam_policy()
        BigQueryAdaptor.patch_dataset_access(user)

        conn
        |> configure_session(drop: true)
        |> put_flash(:info, "Profile deleted!")
        |> redirect(to: Routes.auth_path(conn, :login, team_user_deleted: true))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html", changeset: changeset, team_user: team_user)
    end
  end
end
