defmodule LogflareWeb.CloudflareController do
  use LogflareWeb, :controller

  # import Ecto.Query, only: [from: 2]

  alias Logflare.Repo
  alias Logflare.User

  # TODO: Figure out structs
  # TODO: Remove source on logout response
  # TODO: Put back in source when logging in a second time from CF app

  # defstruct title: "",
  #           type: "string",
  #           description: "Which source should we send logs to?",
  #           order: 2,
  #           required: true,
  #           enum: [],
  #           enumNames: %{}

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
    # response = %{"message" => "Event acknowledged!"}
    install = %{"install" => {}}
    response = conn.params["install"]
    response = put_in(install, ["install"], response)

    # {_pop, response} =
    #   pop_in(
    #     response,
    #     ["install", "schema", "properties", "source"]
    #   )

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

    # build top level json response
    install = %{"install" => {}}
    response = conn.params["install"]
    response = put_in(install, ["install"], response)

    # put in lists of sources for dropdown
    response =
      put_in(
        response,
        ["install", "schema", "properties", "source", "enum"],
        enum
      )

    response =
      put_in(
        response,
        ["install", "schema", "properties", "source", "enumNames"],
        enum_names
      )

    # send over API key for worker
    account_id = owner.resource_owner_id
    account = Repo.get_by(User, id: account_id)
    api_key = account.api_key

    options = conn.params["install"]["options"]
    logflare = %{"logflare" => %{"api_key" => api_key}}
    new_options = Map.merge(options, logflare)
    response = put_in(response, ["install", "options"], new_options)

    {:ok, response}
  end
end
