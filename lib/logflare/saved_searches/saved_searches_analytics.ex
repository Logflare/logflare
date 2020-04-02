defmodule Logflare.SavedSearches.Analytics do
  @moduledoc false
  alias Logflare.SavedSearch
  alias Logflare.SavedSearchCounter
  import Ecto.Query
  alias Logflare.Repo

  def search_timeseries() do
    SavedSearchCounter
    |> join(:left, [c], s in SavedSearch, on: c.saved_search_id == s.id)
    |> group_by([c, s], [fragment("?::DATE", c.timestamp)])
    |> select([c, s], %{
      timestamp: fragment("?::DATE", c.timestamp),
      non_tailing_count: sum(c.non_tailing_count),
      tailing_count: sum(c.tailing_count)
    })
    |> Repo.all()
    |> IO.inspect()
  end

  def saved_searches() do
    SavedSearch
    |> group_by([s], s.saved_by_user)
    |> select([s], %{
      saved_by_user: s.saved_by_user,
      count: count(s.id)
    })
    |> Repo.all()
  end

  def top_field_paths(type) when type in [:lql_filters, :lql_charts] do
    paths =
      SavedSearch
      |> select([s], %{
        path: fragment("jsonb_array_elements(?) -> 'path'", field(s, ^type))
      })
      |> subquery

    paths
    |> from()
    |> group_by([p], p.path)
    |> select([p], %{
      path: p.path,
      count: count(p.path)
    })
    |> order_by(desc: :count)
    |> limit(100)
    |> Repo.all()
  end
end
