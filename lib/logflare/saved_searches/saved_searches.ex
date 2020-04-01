defmodule Logflare.SavedSearches do
  @moduledoc false
  import Ecto.Query
  alias Logflare.{SavedSearch, Repo}
  alias Logflare.Source
  alias Logflare.Lql.{FilterRule, ChartRule}
  alias Logflare.SavedSearchCounter
  alias Logflare.DateTimeUtils

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
    {tcount, ntcount} =
      if tailing? do
        {1, 0}
      else
        {0, 1}
      end

    %SavedSearchCounter{
      saved_search_id: search_id,
      datetime: DateTime.utc_now() |> DateTimeUtils.truncate(:hour),
      tailing_count: 0,
      non_tailing_count: 0,
      granularity: "day"
    }
    |> Repo.insert(
      on_conflict: [inc: [tailing_count: tcount, non_tailing_count: ntcount]],
      conflict_target: [:saved_search_id, :datetime, :granularity]
    )
  end

  def get_by(querystring, source) do
    SavedSearch
    |> where([s], s.querystring == ^querystring)
    |> where([s], s.source_id == ^source.id)
    |> Repo.one()
  end
end
