defmodule LogflareWeb.TeamUserController do
  use LogflareWeb, :controller

  plug LogflareWeb.Plugs.AuthMustBeTeamAdmin when action in [:delete, :update_role]

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Teams.TeamContext
  alias Logflare.TeamUsers
  alias Logflare.Users

  def edit(%{assigns: %{team_user: team_user, user: _user}} = conn, _params) do
    changeset = TeamUsers.TeamUser.changeset(team_user, %{})
    team_user = TeamUsers.preload_defaults(team_user)
    assigns = [changeset: changeset, team_user: team_user]

    render(conn, "edit.html", assigns)
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

  def update_role(%{assigns: %{team_context: team_context}} = conn, %{
        "id" => team_user_id,
        "team_role" => role_params
      }) do
    target_team_user =
      TeamUsers.get_team_user_by(id: team_user_id, team_id: team_context.team.id)

    redirect_path = team_members_redirect_path(team_context)

    if is_nil(target_team_user) do
      conn
      |> put_flash(:error, "Not authorized to update this team member's role")
      |> redirect(to: redirect_path)
    else
      role =
        case role_params do
          %{"is_admin" => "true"} -> "admin"
          _ -> "user"
        end

      case TeamUsers.update_team_role(target_team_user, %{role: role}) do
        {:ok, _team_role} ->
          conn
          |> put_flash(:info, "#{target_team_user.email} role updated.")
          |> redirect(to: redirect_path)

        {:error, _changeset} ->
          conn
          |> put_flash(:error, "Failed to update role!")
          |> redirect(to: redirect_path)
      end
    end
  end

  def delete(
        %{assigns: %{team_context: team_context} = assigns} = conn,
        %{"id" => team_user_id} = _params
      ) do
    redirect_path = team_members_redirect_path(team_context)

    user = Users.preload_team(assigns.user)

    target_team_user =
      TeamUsers.get_team_user_by(id: team_user_id, team_id: team_context.team.id)

    if is_nil(target_team_user) do
      conn
      |> put_flash(:error, "Not authorized to delete this team member")
      |> redirect(to: redirect_path)
    else
      case TeamUsers.delete_team_user(target_team_user) do
        {:ok, _team_user} ->
          BigQueryAdaptor.update_iam_policy()
          BigQueryAdaptor.patch_dataset_access(user)

          conn
          |> put_flash(:info, "Member profile deleted!")
          |> redirect(to: redirect_path)

        {:error, _changeset} ->
          conn
          |> put_flash(:error, "Something went wrong!")
          |> redirect(to: redirect_path)
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

  defp team_members_redirect_path(%TeamContext{} = team_context) do
    if TeamContext.team_owner?(team_context) do
      ~p"/account/edit" <> "?t=#{team_context.team.id}#team-members"
    else
      ~p"/profile/edit" <> "?t=#{team_context.team.id}#team-members"
    end
  end
end
