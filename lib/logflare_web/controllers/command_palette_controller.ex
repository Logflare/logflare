defmodule LogflareWeb.CommandPaletteController do
  use LogflareWeb, :controller

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Teams
  alias Logflare.Teams.Team

  @type source_payload :: %{
          id: pos_integer(),
          name: String.t(),
          favorite: boolean(),
          service_name: String.t() | nil,
          team: %{id: pos_integer(), name: String.t()}
        }

  def sources(conn, _params) do
    payloads =
      case principal(conn) do
        nil ->
          []

        principal ->
          principal
          |> Teams.list_teams_by_user_access()
          |> Enum.flat_map(&team_sources/1)
      end

    json(conn, %{sources: payloads})
  end

  defp principal(conn), do: conn.assigns[:team_user] || conn.assigns[:user]

  defp team_sources(%Team{} = team) do
    [user_id: team.user_id, system_source: false]
    |> Sources.list_sources()
    |> Enum.map(&to_payload(&1, team))
  end

  @spec to_payload(Source.t(), Team.t()) :: source_payload()
  defp to_payload(source, team) do
    %{
      id: source.id,
      name: source.name,
      favorite: source.favorite || false,
      service_name: source.service_name,
      team: %{id: team.id, name: team.name}
    }
  end
end
