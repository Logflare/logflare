defmodule Logflare.SavedSearches do
  @moduledoc false
  import Ecto.Query
  alias Logflare.{SavedSearch, Repo}
  alias Logflare.Source
  alias Logflare.Lql
  alias Logflare.Lql.{FilterRule, ChartRule}
  alias Logflare.SavedSearchCounter
  alias Logflare.DateTimeUtils

  @spec insert(String.t(), [FilterRule.t() | ChartRule.t()], Source.t()) ::
          {:ok, SavedSearch} | {:error, term}
  def insert(querystring, lql_rules, %Source{} = source) do
    lql_filters = Lql.Utils.get_filter_rules(lql_rules)
    lql_charts = Lql.Utils.get_chart_rules(lql_rules)

    source
    |> Ecto.build_assoc(:saved_searches)
    |> SavedSearch.changeset(%{
      querystring: querystring,
      lql_filters: lql_filters,
      lql_charts: lql_charts
    })
    |> Repo.insert()
  end

  def delete(search) do
    Repo.delete(search)
  end

  def delete_by_user(search) do
    search
    |> SavedSearch.changeset(%{saved_by_user: false})
    |> Repo.update()
  end

  def save_by_user(querystring, lql_rules, source) do
    search = get_by_qs_source_id(querystring, source.id)

    if search do
      search
      |> SavedSearch.changeset(%{saved_by_user: true})
      |> Repo.update()
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
      timestamp: DateTime.utc_now() |> DateTimeUtils.truncate(:hour),
      tailing_count: 0,
      non_tailing_count: 0,
      granularity: "day"
    }
    |> Repo.insert(
      on_conflict: [inc: [tailing_count: tcount, non_tailing_count: ntcount]],
      conflict_target: [:saved_search_id, :timestamp, :granularity]
    )
  end

  def get_by_qs_source_id(querystring, source_id) do
    SavedSearch
    |> where([s], s.querystring == ^querystring)
    |> where([s], s.source_id == ^source_id)
    |> Repo.one()
  end

  def mark_as_saved_by_users() do
    SavedSearch
    |> where([s], is_nil(s.saved_by_user))
    |> Repo.update_all(set: [saved_by_user: true])
  end
end
