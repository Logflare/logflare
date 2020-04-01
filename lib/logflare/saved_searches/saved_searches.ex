defmodule Logflare.SavedSearches do
  @moduledoc false
  import Ecto.Query
  alias Logflare.{SavedSearch, Repo}
  alias Logflare.Source
  alias Logflare.Lql.{FilterRule, ChartRule}

  @spec insert(String.t(), [FilterRule.t() | ChartRule.t()], Source.t()) :: :ok | {:error, term}
  def insert(querystring, lql_rules, %Source{} = source) do
    source
    |> Ecto.build_assoc(:saved_searches)
    |> SavedSearch.changeset(%{querystring: querystring, lql: lql_rules})
    |> Repo.insert()
  end

  def delete(search) do
    Repo.delete(search)
  end

  def save_by_user(querystring, lql_rules, source) do
    search = get_by(querystring, source)

    if search do
      Repo.update(search, saved_by_user: true)
    else
      insert(querystring, lql_rules, source)
    end
  end

  def inc(search_id, tailing?: tailing?) do
    search = Repo.get(SavedSearch, search_id)

    cond do
      search && tailing? ->
        Repo.update(search, update: [inc: :count_tailing])

      search ->
        Repo.update(search, update: [inc: :count_non_tailing])
    end
  end

  def get_by(querystring, source) do
    SavedSearch
    |> where([s], s.querystring == ^querystring)
    |> where([s], s.source_id == ^source.id)
    |> Repo.one()
  end
end
