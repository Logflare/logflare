defmodule Logflare.Endpoints do
  @moduledoc false
  alias Logflare.Endpoints.Query
  alias Logflare.Repo
  alias Logflare.User
  @spec get_query_by_token(binary()) :: Query.t() | nil
  def get_query_by_token(token) when is_binary(token) do
    get_by(token: token)
  end

  def get_mapped_query_by_token(token) when is_binary(token) do
    token
    |> get_query_by_token()
    |> case do
      nil -> nil
      query -> Query.map_query(query)
    end
  end

  @spec get_by(Keyword.t()) :: Query.t() | nil
  def get_by(kw) do
    Repo.get_by(Query, kw)
  end

  @spec create_query(User.t(), map()) :: {:ok, Query.t()} | {:error, any()}
  def create_query(user, params) do
    user
    |> Ecto.build_assoc(:endpoint_queries)
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(params)
    |> Repo.insert()
  end

  @doc """
  Creates a sandboxed endpoint. A sandboxed endpoint is an endpoint with a "parent" endpoint containing a CTE.

  This will allow us to query the parent sandbox using a fixed SQL query, without allowing unrestricted sql queries to be made.
  """
  @spec create_sandboxed_query(User.t(), Query.t(), map()) :: {:ok, Query.t()} | {:error, :no_cte}
  def create_sandboxed_query(user, sandbox, attrs) do
    case Logflare.SqlV2.contains_cte?(sandbox.query) do
      true ->
        user
        |> Ecto.build_assoc(:endpoint_queries, sandbox_query: sandbox)
        |> Repo.preload(:user)
        |> Query.sandboxed_endpoint_changeset(attrs)
        |> Repo.insert()

      false ->
        {:error, :no_cte}
    end
  end

  @spec update_query(Query.t(), map()) :: {:ok, Query.t()} | {:error, any()}
  def update_query(query, params) do
    query
    |> Repo.preload(:user)
    |> Query.update_by_user_changeset(params)
    |> Repo.update()
  end

  @spec delete_query(Query.t()) :: {:ok, Query.t()} | {:error, any()}
  def delete_query(query) do
    Repo.delete(query)
  end
end
