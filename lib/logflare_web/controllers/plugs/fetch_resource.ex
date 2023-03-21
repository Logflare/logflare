defmodule LogflareWeb.Plugs.FetchResource do
  @moduledoc """
  Fetch the main relevant route resource based on conn.assigns.resource_type.

  Resource will be set on assigns with resource_type value as key

  For example with conn.assigns.resource_type: :source,
  conn.assigns.source will be fetched and set.

  The resource to be fetched should be set on the route's assigns option in the router.
  """
  import Plug.Conn
  alias Logflare.Sources
  alias Logflare.Endpoints
  def init(_opts), do: nil

  def call(%{assigns: %{resource_type: type}} = conn, _opts) when is_atom(type) do
    conn = fetch_query_params(conn)

    resource =
      case type do
        :source ->
          token = Map.get(conn.params, "source")
          Sources.get_source_by_token(token)

        :endpoint ->
          token = Map.get(conn.params, "token")
          Endpoints.get_query_by_token(token)
      end

    conn
    |> assign(type, resource)
  end

  def call(conn, _), do: conn
end
