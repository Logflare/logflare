defmodule LogflareWeb.Plugs.VerifyApiAccess do
  @moduledoc """
  Verifies if a user has access to a requested resource.

  Assigns the token's associated user if the token is provided

  Authentication api key can either be through access tokens or legacy `user.api_key`.

  Access token usage is preferred and `user.api_key` is only used as a fallback.
  """
  import Plug.Conn
  alias Logflare.Auth
  alias Logflare.Users
  alias Logflare.User
  alias Logflare.Partners.Partner
  alias Logflare.Partners
  alias LogflareWeb.Api.FallbackController

  def init(args), do: args |> Enum.into(%{})

  def call(conn, opts) do
    opts = Enum.into(opts, %{scopes: []})
    resource_type = Map.get(conn.assigns, :resource_type)

    impersonate_user_token = get_req_header(conn, "x-lf-partner-user") |> List.first()
    # generic access
    scopes_to_check =
      if impersonate_user_token != nil do
        ~w(partner)
      else
        opts.scopes
      end

    case identify_requestor(conn, scopes_to_check) do
      {:ok, %Partner{} = partner} when impersonate_user_token == nil ->
        conn
        |> assign(:partner, partner)

      {:ok, %Partner{} = partner} when impersonate_user_token != nil ->
        # maybe get the user target

        Partners.Cache.get_user_by_token(partner, impersonate_user_token)
        |> then(fn
          %User{} = u ->
            conn
            |> assign(:partner, partner)
            |> assign(:user, Users.Cache.preload_defaults(u))

          _ ->
            FallbackController.call(conn, {:error, :unauthorized})
        end)

      {:ok, %User{} = user} ->
        assign(conn, :user, user)

      {:error, :no_token} when resource_type != nil ->
        conn

      _ ->
        FallbackController.call(conn, {:error, :unauthorized})
    end
  end

  defp identify_requestor(conn, scopes) do
    extracted = extract_token(conn)
    is_private_route? = "private" in scopes

    with {:ok, access_token_or_api_key} <- extracted,
         {:ok, %User{} = owner} <- Auth.Cache.verify_access_token(access_token_or_api_key, scopes) do
      {:ok, Users.Cache.preload_defaults(owner)}
    else
      # don't preload for partners
      {:ok, %Partner{}} = res -> res
      {:error, :no_token} = err -> err
      {:error, _} = err -> handle_legacy_api_key(extracted, err, is_private_route?)
    end
  end

  defp handle_legacy_api_key({:ok, api_key}, err, is_private_route?) do
    case Users.Cache.get_by_and_preload(api_key: api_key) do
      %_{} = user when is_private_route? == false -> {:ok, user}
      _ when is_private_route? == false -> {:error, :no_token}
      _ when is_private_route? == true -> {:error, :unauthorized}
      _ -> err
    end
  end

  defp extract_token(conn) do
    auth_header =
      conn
      |> Plug.Conn.get_req_header("authorization")
      |> List.first()

    bearer =
      case auth_header do
        "Bearer " <> token -> token
        _ -> nil
      end

    api_key =
      conn
      |> Plug.Conn.get_req_header("x-api-key")
      |> List.first(conn.params["api_key"])

    cond do
      bearer != nil -> {:ok, bearer}
      api_key != nil -> {:ok, api_key}
      true -> {:error, :no_token}
    end
  end
end
