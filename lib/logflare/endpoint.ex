defmodule Logflare.Endpoint do
  import Ecto.Query
  alias Logflare.Endpoint.Query
  alias Logflare.Repo

  @spec get_query_by_token(binary()) :: %Query{} | nil
  def get_query_by_token(token) when is_binary(token) do
    query =
      from q in Query,
        where: q.token == ^token

    Repo.one(query) |> Query.map_query()
  end
end
