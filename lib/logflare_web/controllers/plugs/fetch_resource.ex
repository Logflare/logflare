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
  alias Logflare.Utils

  def init(_opts), do: nil

  # ingest by source name
  def call(
        %{assigns: %{user: user, resource_type: :source}, params: params} =
          conn,
        _opts
      )
      when is_map_key(params, "source_name") or is_map_key(params, "collection_name") do
    name = params["source_name"] || params["collection_name"]

    source =
      Sources.Cache.get_by_and_preload_rules(name: name, user_id: user.id)
      |> Sources.refresh_source_metrics_for_ingest()

    assign(conn, :source, source)
  end

  # ingest by source token
  def call(%{assigns: %{resource_type: :source}, params: params} = conn, _opts) do
    token =
      Utils.Map.get(params, :source) || Utils.Map.get(params, :collection) ||
        get_source_from_headers(conn)

    source =
      case uuid?(token) do
        true ->
          Sources.Cache.get_by_and_preload_rules(token: token)
          |> Sources.refresh_source_metrics_for_ingest()

        _ ->
          nil
      end

    assign(conn, :source, source)
  end

  def call(
        %{
          assigns: %{resource_type: :endpoint} = assigns,
          params: %{"token_or_name" => token_or_name}
        } = conn,
        _opts
      ) do
    user_id =
      Map.get(assigns, :user)
      |> then(fn
        nil -> nil
        %_{} = user -> user.id
      end)

    endpoint =
      case uuid?(token_or_name) do
        false when user_id != nil ->
          Endpoints.Cache.get_by(name: token_or_name, user_id: user_id)

        true when user_id != nil ->
          Endpoints.Cache.get_by(token: token_or_name, user_id: user_id)

        true when user_id == nil ->
          Endpoints.Cache.get_by(token: token_or_name)

        _ ->
          nil
      end

    assign(conn, :endpoint, endpoint)
  end

  def call(
        %{assigns: %{resource_type: :endpoint, user: %{id: user_id}}, params: %{"name" => name}} =
          conn,
        _opts
      ) do
    endpoint = Endpoints.Cache.get_by(name: name, user_id: user_id)
    assign(conn, :endpoint, endpoint)
  end

  def call(conn, _), do: conn

  # returns true if it is a valid uuid4 string
  defp uuid?(value) when is_binary(value) do
    case Ecto.UUID.dump(value) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp get_source_from_headers(conn) do
    get_first_header(conn, ["x-source", "x-collection"])
  end

  defp get_first_header(conn, headers) do
    Enum.find_value(headers, fn header ->
      conn |> Plug.Conn.get_req_header(header) |> List.first()
    end)
  end
end
