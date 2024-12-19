defmodule LogflareWeb.Plugs.VerifyResourceAccess do
  @moduledoc """
  Plug that checks for ownership of the a provided resource.

  If the `:user` assign is not set, verification is assumed to have passed and as passthroguh is performed.
  Also checks any API scopes that are set.
  If no resource is set, performs a passthrough.
  """
  alias Logflare.Source
  alias Logflare.Endpoints.Query
  alias Logflare.User
  alias Logflare.Auth
  alias LogflareWeb.Api.FallbackController
  def init(_opts), do: nil

  def call(%{assigns: %{endpoint: %Query{enable_auth: false}}} = conn, _opts) do
    conn
  end

  # check source
  def call(
        %{
          assigns: %{
            access_token: access_token,
            user: %User{id: id},
            source: %Source{id: source_id, user_id: user_id}
          }
        } = conn,
        _opts
      )
      when id == user_id do
    if :ok == Auth.check_scopes(access_token, ["ingest", "ingest:source:#{source_id}"]) or
         :ok == Auth.check_scopes(access_token, ["ingest", "ingest:collection:#{source_id}"]) do
      conn
    else
      FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  # check endpoint
  def call(
        %{
          assigns: %{
            access_token: access_token,
            user: %User{id: id},
            endpoint: %Query{id: endpoint_id, user_id: user_id}
          }
        } = conn,
        _opts
      )
      when id == user_id do
    if :ok == Auth.check_scopes(access_token, ["query", "query:endpoint:#{endpoint_id}"]) do
      conn
    else
      FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  # halts all others
  def call(%{assigns: assigns} = conn, _) when is_map_key(assigns, :resource_type) do
    FallbackController.call(conn, {:error, :unauthorized})
  end

  # no resource is set, passthrough
  def call(conn, _), do: conn
end
