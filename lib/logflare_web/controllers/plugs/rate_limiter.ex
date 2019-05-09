defmodule LogflareWeb.Plugs.RateLimiter do
  @moduledoc """
  A plug that allows or denies API action based on the API request rate rules for user/source
  """
  import Plug.Conn

  def init(_params) do
  end

  def call(conn, params) do
    user = conn.assigns.user
    source_token = params["source"]
    source_name = params["source_name"]

    allowed =
      users_api().action_allowed?(%{
        user: user,
        source: %{
          id: source_token,
          name: source_name
        },
        type: {:api_call, :logs_post}
      })

    if allowed do
      conn
    else
      conn
      |> send_resp(429, "rate limit")
      |> halt()
    end
  end

  def users_api do
    if Mix.env() == :test do
      Logflare.Users.APIMock
    else
      Logflare.Users.API
    end
  end
end
