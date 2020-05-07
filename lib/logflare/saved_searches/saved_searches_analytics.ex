defmodule Logflare.SavedSearches.Analytics do
  @moduledoc false
  alias Logflare.SavedSearch
  alias Logflare.SavedSearchCounter
  import Ecto.Query
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.User

  def source_timeseries() do
    search_counters_with_sources()
    |> group_by_date()
    |> select([c, s, source], %{
      timestamp: fragment("?::DATE", c.timestamp),
      count: fragment("count(distinct ?)", source.id)
    })
    |> order_by_date_default()
    |> Repo.all()
  end

  def top_sources(:"24h") do
    search_counters_with_sources()
    |> select([c, s, source], %{
      id: source.id,
      name: source.name,
      tailing_count: sum(c.tailing_count),
      non_tailing_count: sum(c.non_tailing_count)
    })
    |> where([c, ...], c.timestamp >= ago(24, "hour"))
    |> group_by([c, s, source], source.id)
    |> order_by([c, ...], desc: sum(c.tailing_count))
    |> Repo.all()
  end

  def user_timeseries() do
    search_counters_with_users()
    |> group_by_date()
    |> select([c, s, source, user], %{
      timestamp: fragment("?::DATE", c.timestamp),
      count: fragment("count(distinct ?)", user.id)
    })
    |> order_by_date_default()
    |> Repo.all()
  end

  def search_timeseries() do
    SavedSearchCounter
    |> join(:left, [c], s in SavedSearch, on: c.saved_search_id == s.id)
    |> group_by_date()
    |> select([c, s], %{
      timestamp: fragment("?::DATE", c.timestamp),
      non_tailing_count: sum(c.non_tailing_count),
      tailing_count: sum(c.tailing_count)
    })
    |> order_by_date_default()
    |> Repo.all()
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

  def operators() do
    operators =
      SavedSearch
      |> select([s], %{
        operator: fragment("jsonb_array_elements(?) -> 'operator'", s.lql_filters),
        saved_search_id: s.id
      })
      |> subquery()

    total =
      from(operators)
      |> select([o], fragment("COUNT(DISTINCT(?))", o.saved_search_id))
      |> Repo.one()

    from(operators)
    |> group_by([o], o.operator)
    |> select([o], %{
      operator: o.operator,
      searches_with_operator_share:
        fragment("COUNT(DISTINCT(?))", o.saved_search_id) / type(^total, :float) * 100
    })
    |> order_by(desc: 2)
    |> Repo.all()
  end

  def search_counters_with_sources() do
    SavedSearchCounter
    |> join(:left, [c], s in SavedSearch, on: c.saved_search_id == s.id)
    |> join(:left, [c, s], source in Source, on: s.source_id == source.id)
  end

  def search_counters_with_users() do
    search_counters_with_sources()
    |> join(:left, [c, s, source], user in User, on: source.user_id == user.id)
  end

  def group_by_date(q) do
    group_by(q, [c, ...], [fragment("?::DATE", c.timestamp)])
  end

  defp order_by_date_default(q) do
    order_by(q, [c, ...], asc: fragment("?::DATE", c.timestamp))
  end
end
