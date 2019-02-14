defmodule LogflareWeb.CloudflareController do
  use LogflareWeb, :controller

  # import Ecto.Query, only: [from: 2]

  alias Logflare.Source
  alias Logflare.Repo
  alias Logflare.User

  def event(conn, params) do
    # event(conn, %{"authentications" => %{"account" => %{"token" => %{"token" => user_token}}}})

    user_token = params["authentications"]["account"]["token"]["token"]
    {:ok, response} = build_response(conn, user_token)

    response = Jason.encode!(response)

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, response)
  end

  defp build_response(conn, user_token) when is_nil(user_token) do
    response = %{"message" => "Event acknowledged!"}
    install = %{"install" => {}}
    response = conn.params["install"]
    response = put_in(install, ["install"], response)

    {_pop, response} =
      pop_in(
        response,
        ["install", "schema", "properties", "accountSources"]
      )

    {:ok, response}
  end

  defp build_response(conn, user_token) do
    owner =
      Logflare.Repo.get_by(ExOauth2Provider.OauthAccessTokens.OauthAccessToken, token: user_token)

    sources = Repo.all(Ecto.assoc(owner, [:resource_owner, :sources]))
    enum = Enum.map(sources, & &1.token)

    enum_names =
      Enum.map(sources, fn x -> {x.token, x.name} end)
      |> Map.new()

    install = %{"install" => {}}
    response = conn.params["install"]

    response = put_in(install, ["install"], response)

    response =
      put_in(
        response,
        ["install", "schema", "properties", "accountSources", "enum"],
        enum
      )

    response =
      put_in(
        response,
        ["install", "schema", "properties", "accountSources", "enumNames"],
        enum_names
      )

    {:ok, response}
  end
end
