defmodule LogflareWeb.CloudflareControllerV1 do
  @moduledoc """
  Handles webhooks from Cloudlfare.
  """
  use LogflareWeb, :controller

  alias Logflare.JSON
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.OauthAccessTokens.OauthAccessToken

  @doc """
  Takes the conn and params and builds a response based on the authentication token from Cloudflare.
  """
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
    |> reset_properties()
  end

  defp build_response(conn, user_token) do
    owner = Logflare.Repo.get_by(OauthAccessToken, token: user_token)
    sources = Repo.all(Ecto.assoc(owner, [:resource_owner, :sources]))
    enum = Enum.map(sources, & &1.token)
    response = init_json(conn)
    new_options = build_options(owner, response)
    default_source = List.first(enum)
    new_properties = build_properties(response)

    enum_names =
      Enum.map(sources, fn x -> {x.token, x.name} end)
      |> Map.new()

    put_in(response, ["install", "schema", "properties"], new_properties)
    |> put_in(["install", "schema", "properties", "source", "enum"], enum)
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
          "title" => "Logflare dashboard",
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

    metadata = %{
      "metadata" => [
        %{"field" => "rMeth"},
        %{"field" => "statusCode"},
        %{"field" => "cIP"},
        %{"field" => "cfRay"},
        %{"field" => "rUrl"},
        %{"field" => "uAgent"}
      ]
    }

    Map.merge(options, logflare)
    |> Map.merge(metadata)
  end

  defp build_properties(response) do
    properties = response["install"]["schema"]["properties"]

    metadata = %{
      "metadata" => %{
        "title" => "Request Metadata",
        "type" => "array",
        "description" => "Customise your log messages. Add or remove fields and set the order.",
        "items" => %{
          "type" => "object",
          "properties" => %{
            "field" => %{
              "title" => "Field",
              "type" => "string",
              "enum" => [
                "rMeth",
                "statusCode",
                "rUrl",
                "cfRay",
                "cIP",
                "uAgent",
                "cfCacheStatus",
                "contentLength",
                "contentType",
                "responseConnection",
                "requestConnection",
                "cacheControl",
                "acceptRanges",
                "expectCt",
                "expires",
                "lastModified",
                "vary",
                "server",
                "etag",
                "date",
                "transferEncoding"
              ],
              "enumNames" => %{
                "rMeth" => "Request Method",
                "statusCode" => "Status Code",
                "rUrl" => "Request URL",
                "cfRay" => "Ray ID",
                "cIP" => "Connecting IP",
                "uAgent" => "User Agent",
                "cfCacheStatus" => "Cache Status",
                "contentLength" => "Content Length",
                "contentType" => "Content Type",
                "responseConnection" => "Response Connection",
                "requestConnection" => "Request Connection",
                "cacheControl" => "Cache Control",
                "acceptRanges" => "Accept Ranges",
                "expectCt" => "Expect CT",
                "expires" => "Expires",
                "lastModified" => "Last Modified",
                "vary" => "Vary",
                "server" => "Server",
                "etag" => "Etag",
                "date" => "Date",
                "transferEncoding" => "Transfer Encoding"
              },
              "default" => "statusCode"
            }
          }
        }
      }
    }

    Map.merge(properties, metadata)
  end

  defp reset_properties(response) do
    metadata = %{}

    put_in(response, ["install", "schema", "properties", "metadata"], metadata)
  end

  defp reset_options(response) do
    {_pop, response} = pop_in(response, ["install", "options", "logflare"])
    {_pop, response} = pop_in(response, ["install", "options", "metadata"])

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
