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

    result =
      users_api().verify_api_rates_quotas(%{
        user: user,
        source: %{
          id: source_token,
          name: source_name
        },
        type: {:api_call, :logs_post}
      })

    case result do
      {:ok, %{metrics: metrics}} ->
        conn
        |> put_x_rate_limit_headers(metrics)

      {:error, %{message: message, metrics: metrics}} ->
        conn
        |> put_x_rate_limit_headers(metrics)
        |> send_resp(429, message)
        |> halt()
    end
  end

  def put_x_rate_limit_headers(conn, metrics) do
    metrics = Iteraptor.map(metrics, fn {_, int} -> Integer.to_string(int) end)

    conn
    |> put_resp_header("x-rate-limit-user_limit", metrics.user.limit)
    |> put_resp_header("x-rate-limit-user_remaining", metrics.user.remaining)
    |> put_resp_header("x-rate-limit-source_limit", metrics.source.limit)
    |> put_resp_header("x-rate-limit-source_remaining", metrics.source.remaining)
  end

  def users_api do
    if Mix.env() == :test do
      Logflare.Users.APIMock
    else
      Logflare.Users.API
    end
  end
end
