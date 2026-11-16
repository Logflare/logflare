defmodule LogflareWeb.PhoenixOauth2Provider.AuthorizationView do
  use LogflareWeb, :view

  alias ExOauth2Provider.Mixin.Scopes, as: ScopeDefaults
  alias ExOauth2Provider.Scopes

  @spec consent_scopes(map(), [String.t()], keyword()) :: [String.t()]
  def consent_scopes(client, [], config) do
    client.scopes
    |> ScopeDefaults.parse_default_scope_string(config)
    |> Scopes.to_list()
  end

  def consent_scopes(_client, scopes, _config), do: scopes
end
