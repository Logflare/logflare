defmodule LogflareWeb.Live.ResourceTeamContext do
  @doc """
  Callback to provide team id query for requested resource.

  Intended for normalising the current team context URL param and not
  for verifying resource access control.
  """
  @callback resource_team_id_query(
              params :: map(),
              uri :: String.t(),
              user :: Logflare.User.t() | Logflare.TeamUsers.TeamUser.t()
            ) :: Ecto.Query.t() | nil
end
