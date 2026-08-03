defmodule LogflareWeb.Auth.VercelAuth do
  @moduledoc """
  What we do when someone signs up and oauth's from the Vercel integration.
  """
  use LogflareWeb, :controller

  alias Logflare.Vercel
  alias LogflareWeb.Router.Helpers, as: Routes

  defp env_config, do: Application.get_env(:logflare, __MODULE__)

  def set_oauth_params(%{query_string: query_string} = conn, _params) do
    redirect_uri = "#{env_config()[:vercel_app_host]}/api/callback?#{query_string}"

    params = %{
      "client_id" => "#{env_config()[:client_id]}",
      "redirect_uri" => redirect_uri,
      "scope" => "read write",
      "response_type" => "code"
    }

    conn
    |> put_session(:oauth_params, params)
    |> redirect(to: Routes.auth_path(conn, :login))
  end

  def set_oauth_params_v2(conn, %{"code" => code} = params) do
    {:ok, resp} =
      Vercel.Client.new()
      |> Vercel.Client.get_access_token(code)

    auth_params =
      resp.body
      |> Map.delete("user_id")
      |> Map.put("vercel_user_id", resp.body["user_id"])

    redirect_to =
      if params["next"],
        do: params["next"],
        else: Routes.vercel_log_drains_url(LogflareWeb.Endpoint, :edit)

    params = %{
      "next" => redirect_to,
      "auth_params" => auth_params
    }

    login(conn, params)
  end

  defp login(conn, params) do
    conn
    |> put_session(:vercel_setup, params)
    |> redirect(to: Routes.auth_path(conn, :login))
  end
end
