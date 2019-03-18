defmodule LogflareWeb.Plugs.SetApiUser do
  import Plug.Conn

  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.AccountCache

  def init(_params) do
  end

  def call(conn, _params) do
    headers = Enum.into(conn.req_headers, %{})
    api_key = headers["x-api-key"]

    case AccountCache.verify_account?(api_key) do
      true ->
        cond do
          user = api_key && Repo.get_by(User, api_key: api_key) ->
            assign(conn, :user, user)

          true ->
            assign(conn, :user, nil)
        end

      false ->
        assign(conn, :user, nil)
    end
  end
end
