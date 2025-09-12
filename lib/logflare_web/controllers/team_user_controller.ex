defmodule LogflareWeb.TeamUserController do
  use LogflareWeb, :controller
  use Phoenix.HTML

  alias Logflare.TeamUsers
  alias Logflare.Users
  alias Logflare.Backends.Adaptor.BigQueryAdaptor

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

  def change_team(
        %{assigns: %{team_user: _team_user, user: _user}} = conn,
        %{
          "user_id" => user_id,
          "team_user_id" => team_user_id
        } = params
      ) do
    new_team_user = TeamUsers.get_team_user!(team_user_id)

    conn
    |> put_session(:user_id, user_id)
    |> put_session(:team_user_id, team_user_id)
    |> put_resp_cookie("_logflare_user_id", user_id, max_age: 2_592_000)
    |> put_resp_cookie("_logflare_team_user_id", team_user_id, max_age: 2_592_000)
    |> put_resp_cookie("_logflare_last_team", "#{new_team_user.team_id}", max_age: 2_592_000)
    |> put_flash(:info, "Welcome to this Logflare team!")
    |> redirect(to: Map.get(params, "redirect_to", ~p"/dashboard"))
  end

  def change_team(
        %{assigns: %{team_user: _team_user, user: user}} = conn,
        %{
          "user_id" => user_id
        } = params
      ) do
    user = Users.preload_team(user)

    conn
    |> put_session(:user_id, user_id)
    |> delete_session(:team_user_id)
    |> delete_resp_cookie("_logflare_user_id")
    |> delete_resp_cookie("_logflare_team_user_id")
    |> put_resp_cookie("_logflare_last_team", "#{user.team.id}", max_age: 2_592_000)
    |> put_flash(:info, "Welcome to this Logflare team!")
    |> redirect(to: Map.get(params, "redirect_to", ~p"/dashboard"))
  end

  def change_team(
        %{assigns: %{user: user}} = conn,
        %{
          "user_id" => user_id,
          "team_user_id" => team_user_id
        } = params
      ) do
    {:ok, team_user} = TeamUsers.update_team_user_on_change_team(user, team_user_id)

    conn
    |> put_session(:user_id, user_id)
    |> put_session(:team_user_id, team_user_id)
    |> put_resp_cookie("_logflare_user_id", user_id, max_age: 2_592_000)
    |> put_resp_cookie("_logflare_team_user_id", team_user_id, max_age: 2_592_000)
    |> put_resp_cookie("_logflare_last_team", "#{team_user.team_id}", max_age: 2_592_000)
    |> put_flash(:info, "Welcome to this Logflare team!")
    |> redirect(to: Map.get(params, "redirect_to", ~p"/dashboard"))
  end
end
