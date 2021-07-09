defmodule LogflareWeb.EndpointController do
  use LogflareWeb, :controller
  alias Logflare.Logs.IngestTypecasting

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @max_results 10_000

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
    case Logflare.SQL.parameters(endpoint_query.query) do
      {:ok, parameters} ->
        case Logflare.SQL.transform(endpoint_query.query, endpoint_query.user_id) do
          {:ok, query} ->
            params = Enum.map(parameters, fn x  ->
              %{
                name: x,
                parameterValue: %{
                  value: conn.query_params[x],
                },
                parameterType: %{
                  type: "STRING",
                }
               }
            end)
            case Logflare.BqRepo.query_with_sql_and_params(@project_id, query, params,
                                  parameterMode: "NAMED", maxResults: @max_results) do
              {:ok, result} ->
                 render(conn, "query.json", result: result.rows)
              {:error, err} ->
                 error = Jason.decode!(err.body)["error"] |> process_error(endpoint_query.user_id)
                 render(conn, "query.json", error: error)
            end
          {:error, err} ->
            render(conn, "query.json", error: err)
        end
      {:error, err} ->
        render(conn, "query.json", error: err)
    end
  end

  defp process_error(error, user_id) do
    error = %{error | "message" => process_message(error["message"], user_id)}
    if is_list(error["errors"]) do
      %{error | "errors" => Enum.map(error["errors"], fn err -> process_error(err, user_id) end)}
    else
      error
    end
  end


  defp process_message(message, user_id) do
    regex = ~r/#{@project_id}\.#{user_id}_#{Mix.env}\.(?<uuid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})/
    names = Regex.named_captures(regex, message)
    case names do
      %{"uuid" => uuid} ->
        uuid = String.replace(uuid, "_", "-")
        query = from s in Logflare.Source,
                where: s.token == ^uuid and s.user_id == ^user_id,
                select: s.name
        case Logflare.Repo.one(query) do
          nil -> message
          name ->
            Regex.replace(regex, message, name)
        end
      _ -> message
    end
  end

end
