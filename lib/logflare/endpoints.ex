defmodule Logflare.Endpoints do
  @moduledoc false
  import Ecto.Query
  alias Logflare.Endpoints.Query
  alias Logflare.Repo

  @spec get_query_by_token(binary()) :: Query.t() | nil
  def get_query_by_token(token) when is_binary(token) do
    query =
      from q in Query,
        where: q.token == ^token

    Repo.one(query) |> Query.map_query()
  end

  @spec get_by(Keyword.t()) :: Query.t() | nil
  def get_by(kw) do
    Repo.get_by(Query, kw)
  end
end
