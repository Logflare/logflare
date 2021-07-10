defmodule LogflareWeb.EndpointController do
  use LogflareWeb, :controller
  alias Logflare.Logs.IngestTypecasting

  import Ecto.Query, only: [from: 2]

  plug CORSPlug,
       [
         origin: "*",
         max_age: 1_728_000,
         headers: [
           "Authorization",
           "Content-Type",
           "Content-Length",
           "X-Requested-With",
           "X-API-Key",
         ],
         methods: ["GET", "POST", "OPTIONS"],
         send_preflight_response?: true
       ]

  def query(%{params: %{"token" => token}} = conn, _) do
    query = from q in Logflare.Endpoint.Query,
            where: q.token == ^token
    endpoint_query = Logflare.Repo.one(query)
    case Logflare.Endpoint.Cache.resolve(endpoint_query) |>
         Logflare.Endpoint.Cache.query(conn.query_params) do
      {:ok, result} ->
         render(conn, "query.json", result: result.rows)
      {:error, err} ->
         render(conn, "query.json", error: err)
    end
  end

end
