defmodule LogflareWeb.CloudflareController do
  use LogflareWeb, :controller

  alias Logflare.JSON
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.OauthAccessTokens.OauthAccessToken

  def event(conn, params) do
    user_token = params["authentications"]["account"]["token"]["token"]

    response =
      build_response(conn, user_token)
      |> JSON.encode!()

    conn
    |> put_resp_content_type("application/json")
    |> send_resp(200, response)
  end

  defp build_response(conn, user_token) when is_nil(user_token) do
    init_json(conn)
    |> reset_options()
    |> reset_sources()
    |> reset_links()
  end

  defp build_response(conn, user_token) do
    owner = Logflare.Repo.get_by(OauthAccessToken, token: user_token)
    sources = Repo.all(Ecto.assoc(owner, [:resource_owner, :sources]))
    enum = Enum.map(sources, & &1.token)
    response = init_json(conn)
    new_options = build_options(owner, response)
    default_source = List.first(enum)

    enum_names =
      Enum.map(sources, fn x -> {x.token, x.name} end)
      |> Map.new()

    put_in(response, ["install", "schema", "properties", "source", "enum"], enum)
    |> put_in(["install", "schema", "properties", "source", "enumNames"], enum_names)
    |> put_in(["install", "options"], new_options)
    |> put_in(["install", "options", "source"], default_source)
  end

  defp init_json(conn) do
    install = %{"install" => {}}
    request = conn.params["install"]

    links = %{
      "links" => [
        %{
          "title" => "Logflare Dashboard",
          "description" => "Signed in successfully! See your logs at your Logflare dashboard.",
          "href" => "https://logflare.app/dashboard"
        }
      ]
    }

    request = Map.merge(request, links)

    put_in(install, ["install"], request)
  end

  defp build_options(owner, response) do
    account_id = owner.resource_owner_id
    account = Repo.get_by(User, id: account_id)
    api_key = account.api_key

    options = response["install"]["options"]
    logflare = %{"logflare" => %{"api_key" => api_key}}

    Map.merge(options, logflare)
  end

  defp reset_options(response) do
    {_pop, response} = pop_in(response, ["install", "options", "logflare"])

    put_in(response, ["install", "options", "source"], "signin")
  end

  defp reset_links(response) do
    links = [
      %{
        "title" => "Logflare",
        "description" => "Need help?",
        "href" => "https://logflare.app/"
      }
    ]

    put_in(response, ["install", "links"], links)
  end

  defp reset_sources(response) do
    source = %{
      "showIf" => %{"account" => %{"op" => "!=", "value" => ""}},
      "default" => "signin",
      "description" => "Which source should we send logs to?",
      "enum" => ["signin"],
      "enumNames" => %{
        "signin" => "Sign in to select a source"
      },
      "order" => 2,
      "productDefinitions" => [],
      "required" => true,
      "title" => "Source",
      "type" => "string"
    }

    put_in(response, ["install", "schema", "properties", "source"], source)
  end
end
