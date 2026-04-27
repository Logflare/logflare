defmodule LogflareWeb.CommandPaletteController do
  use LogflareWeb, :controller

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Teams
  alias Logflare.Teams.Team
  alias LogflareWeb.Utils

  @type source_payload :: %{
          id: pos_integer(),
          name: String.t(),
          favorite: boolean(),
          service_name: String.t() | nil,
          path: String.t(),
          team: %{id: pos_integer(), name: String.t()}
        }

  def sources(conn, _params) do
    teams = Teams.list_teams_by_user_access(current_user(conn))
    teams_by_user_id = Map.new(teams, &{&1.user_id, &1})

    payloads =
      [user_id: Enum.map(teams, & &1.user_id), system_source: false]
      |> Sources.list_sources()
      |> Enum.map(&to_payload(&1, Map.fetch!(teams_by_user_id, &1.user_id)))

    json(conn, %{sources: payloads})
  end

  defp current_user(conn), do: conn.assigns[:team_user] || conn.assigns[:user]

  @spec to_payload(Source.t(), Team.t()) :: source_payload()
  defp to_payload(source, team) do
    %{
      id: source.id,
      name: source.name,
      favorite: source.favorite || false,
      service_name: source.service_name,
      path: Utils.with_team_param("/sources/#{source.id}", team),
      team: %{id: team.id, name: team.name}
    }
  end
end
