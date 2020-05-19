defmodule LogflareWeb.Plugs.RateLimiter do
  @moduledoc """
  A plug that allows or denies API action based on the API request rate rules for user/source
  """
  alias Logflare.{Users}
  import Plug.Conn

  def init(_opts), do: nil

  def call(%{assigns: %{user: user, source: source, plan: plan}} = conn, _opts \\ []) do
    %{
      user: user,
      source: source,
      plan: plan,
      type: {:api_call, :logs_post}
    }
    |> Users.API.verify_api_rates_quotas()
    |> case do
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
end
