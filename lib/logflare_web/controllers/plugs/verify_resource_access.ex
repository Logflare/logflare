defmodule LogflareWeb.Plugs.VerifyResourceAccess do
  @moduledoc """
  Plug that checks for ownership of the a provided resource.

  If the `:user` assign is not set, verification is assumed to have passed and as passthroguh is performed.
  Also checks any API scopes that are set.
  If no resource is set, performs a passthrough.
  """
  alias Logflare.Sources.Source
  alias Logflare.Endpoints.EndpointQuery
  alias Logflare.User
  alias Logflare.Auth
  alias LogflareWeb.Api.FallbackController
  def init(_opts), do: nil

  def call(%{assigns: %{endpoint: %EndpointQuery{enable_auth: false}}} = conn, _opts) do
    conn
  end

  # check source
  def call(
        %{
          assigns:
            %{
              user: %User{id: id},
              source: %Source{user_id: user_id}
            } = assigns
        } = conn,
        _opts
      )
      when id == user_id do
    if check_resource(assigns.source, Map.get(assigns, :access_token)) do
      conn
    else
      FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  # check endpoint
  def call(
        %{
          assigns:
            %{
              user: %User{id: id},
              endpoint: %EndpointQuery{id: endpoint_id, user_id: user_id}
            } = assigns
        } = conn,
        _opts
      )
      when id == user_id do
    access_token = Map.get(assigns, :access_token)

    cond do
      # legacy api key
      access_token == nil ->
        conn

      :ok == Auth.check_scopes(access_token, ["query", "query:endpoint:#{endpoint_id}"]) ->
        conn

      true ->
        FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  # Passthrough when no query-param source was resolved but the request
  # declared sources per-event via __LF_SOURCE; VerifyDeclaredSources already
  # ran and verified ownership on those.
  def call(
        %{assigns: %{resource_type: :source, source: nil, declared_sources: declared}} = conn,
        _
      )
      when map_size(declared) > 0 do
    conn
  end

  def call(%{assigns: assigns} = conn, _) when is_map_key(assigns, :resource_type) do
    FallbackController.call(conn, {:error, :unauthorized})
  end

  # no resource is set, passthrough
  def call(conn, _), do: conn

  @doc """
  Checks that a user owns a source and (if an access token is present) that
  the token's scopes cover ingestion for that source.
  """
  @spec verify_source_access(Source.t(), User.t(), String.t() | nil) :: boolean()
  def verify_source_access(%Source{user_id: source_user_id} = source, %User{id: user_id}, token)
      when source_user_id == user_id do
    check_resource(source, token)
  end

  def verify_source_access(%Source{}, %User{}, _token), do: false

  def check_resource(%Source{}, nil), do: true

  def check_resource(%Source{} = resource, token) do
    :ok ==
      Auth.check_scopes(token, [
        "ingest",
        "ingest:source:#{resource.id}",
        "ingest:collection:#{resource.id}"
      ])
  end
end
