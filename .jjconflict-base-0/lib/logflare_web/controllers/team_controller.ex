defmodule LogflareWeb.TeamController do
  use LogflareWeb, :controller

  alias Logflare.Teams.Team
  alias Logflare.Teams.TeamContext

  def switch(conn, %{"team_id" => team_id, "redirect_to" => redirect_to}) do
    email = get_session(conn, :current_email)

    case TeamContext.resolve(team_id, email) do
      {:ok, %{team: team}} ->
        conn
        |> put_session(:last_switched_team_id, team.id)
        |> redirect(to: index_path(redirect_to, team))

      {:error, _reason} ->
        conn
        |> put_flash(:error, "Unable to switch to that team.")
        |> redirect(to: redirect_to)
    end
  end

  @spec index_path(String.t(), Team.t()) :: String.t()
  defp index_path(redirect_to, team) do
    redirect_to
    |> URI.parse()
    |> Map.get(:path)
    |> resource_index_path()
    |> LogflareWeb.Utils.with_team_param(team)
  end

  @spec resource_index_path(String.t() | nil) :: String.t()
  defp resource_index_path("/sources" <> _), do: ~p"/dashboard"
  defp resource_index_path("/endpoints" <> _), do: ~p"/endpoints"
  defp resource_index_path("/alerts" <> _), do: ~p"/alerts"
  defp resource_index_path(_), do: ~p"/dashboard"
end
