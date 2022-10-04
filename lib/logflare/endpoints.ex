defmodule Logflare.Endpoints do
  @moduledoc false
  import Ecto.Query
  alias __MODULE__.Query
  alias Logflare.Repo

  @spec get_query_by_token(binary()) :: Query.t() | nil
  def get_query_by_token(token) when is_binary(token) do
    query =
      from q in Query,
        where: q.token == ^token

    Repo.one(query) |> Query.map_query()
  end
end
