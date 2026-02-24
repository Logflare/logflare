defmodule LogflareWeb.TeamController do
  use LogflareWeb, :controller

  alias Logflare.Teams.TeamContext

  def switch(conn, %{"team_id" => team_id, "redirect_to" => redirect_to}) do
    email = get_session(conn, :current_email)

    case TeamContext.resolve(team_id, email) do
      {:ok, %{team: team}} ->
        conn
        |> put_session(:last_switched_team_id, team.id)
        |> redirect(to: LogflareWeb.Utils.with_team_param(redirect_to, team))

      {:error, _reason} ->
        conn
        |> put_flash(:error, "Unable to switch to that team.")
        |> redirect(to: redirect_to)
    end
  end
end
