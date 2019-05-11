defmodule LogflareWeb.Plugs.SetApiUser do
  import Plug.Conn

  # alias Logflare.Repo
  # alias Logflare.User
  alias Logflare.AccountCache

  def init(_params) do
  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    case verify_account?(api_key) do
      true ->
        assign(conn, :user, api_key)

      false ->
        assign(conn, :user, nil)
    end
  end

  def verify_account?(api_key) do
    mod =
      if Mix.env() == :test do
        Logflare.AccountCacheMock
      else
        Logflare.AccountCache
      end

    mod.verify_account?(api_key)
  end
end
