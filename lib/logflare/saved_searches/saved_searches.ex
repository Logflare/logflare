defmodule Logflare.SavedSearches do
  @moduledoc false
  import Ecto.Query
  alias Logflare.{SavedSearch, Repo}
  alias Logflare.{Source}
  alias Logflare.Lql
  alias Logflare.SavedSearchCounter
  alias Logflare.DateTimeUtils
  alias Logflare.Lql.{ChartRule, FilterRule}
  require Logger

  @type lql_rule :: ChartRule.t() | FilterRule.t()

  @doc """
  Retrives a SavedSearch by id
  """
  @spec get(number()) :: SavedSearch.t()
  def get(id) do
    Repo.get(SavedSearch, id)
  end

  @doc """
  Inserts a SavedSearch.
  """
  @typep insert_params :: %{
           :lql_rules => list(lql_rule()),
           :querystring => String.t(),
           optional(:saved_by_user) => boolean(),
           optional(:tailing) => boolean()
         }
  @spec insert(insert_params(), Source.t()) :: {:ok, SavedSearch} | {:error, term}
  def insert(%{lql_rules: lql_rules} = params, %Source{} = source) do
    lql_filters = Lql.Utils.get_filter_rules(lql_rules)
    lql_charts = Lql.Utils.get_chart_rules(lql_rules)

    source
    |> Ecto.build_assoc(:saved_searches)
    |> SavedSearch.changeset(
      Map.merge(
        params,
        %{
          lql_filters: lql_filters,
          lql_charts: lql_charts
        }
      )
    )
    |> Repo.insert()
  end

  @doc """
  Completely deletes a saved search.
  TODO: remove, unused.
  """
  @spec delete(SavedSearch.t()) :: {:ok, SavedSearch.t()}
  def delete(search) do
    Repo.delete(search)
  end

  @doc """
  Marks a SavedSearch as not saved (i.e. user will not see in dashboard)
  """
  @spec delete_by_user(SavedSearch.t()) :: {:ok, SavedSearch.t()}
  def delete_by_user(search) do
    search
    |> SavedSearch.changeset(%{saved_by_user: false})
    |> Repo.update()
  end

  @doc """
  Saves a search. Delegates to `insert/2` to perform insertion.
  Checks if search had been previously executed. If so, does not insert a duplicate.
  """
  @spec save_by_user(String.t(), [lql_rule()], Source.t(), boolean()) :: {:ok, SavedSearch.t()}
  def save_by_user(querystring, lql_rules, source, tailing?) do
    search = get_by_qs_source_id(querystring, source.id)

    if search do
      search
      |> SavedSearch.changeset(%{saved_by_user: true, tailing: tailing?})
      |> Repo.update()
    else
      insert(
        %{
          querystring: querystring,
          lql_rules: lql_rules,
          saved_by_user: true,
          tailing: tailing?
        },
        source
      )
    end
  end

  @doc """
  Used for tracking saved search usage.
  """
  @typep inc_opts :: [tailing: boolean(), timestamp: DateTime.t()]
  @spec inc(number(), inc_opts()) :: SavedSearchCounter.t()
  def inc(search_id, opts) do
    tailing? = opts[:tailing]
    timestamp = opts[:timestamp] || DateTime.utc_now() |> DateTimeUtils.truncate(:hour)

    {tcount, ntcount} =
      if tailing? do
        {1, 0}
      else
        {0, 1}
      end

    %SavedSearchCounter{
      saved_search_id: search_id,
      timestamp: timestamp,
      tailing_count: tcount,
      non_tailing_count: ntcount
    }
    |> Repo.insert(
      on_conflict: [inc: [tailing_count: tcount, non_tailing_count: ntcount]],
      conflict_target: [:saved_search_id, :timestamp, :granularity]
    )
  end

  @doc """
  Get a SavedSearch by query string and source
  """
  @spec get_by_qs_source_id(String.t(), number()) :: SavedSearch.t()
  def get_by_qs_source_id(querystring, source_id) do
    SavedSearch
    |> where([s], s.querystring == ^querystring)
    |> where([s], s.source_id == ^source_id)
    |> Repo.one()
  end

  @doc """
  Retrieves similar SavedSearch to the current query string, scoped to a source.
  """
  @spec suggest_saved_searches(String.t(), number()) :: [SavedSearch.t()]
  def suggest_saved_searches(querystring, source_id) do
    qs = "%#{querystring}%"

    SavedSearch
    |> where([s], ilike(s.querystring, ^qs))
    |> where([s], s.source_id == ^source_id)
    |> order_by([s], desc: s.inserted_at)
    |> limit([s], 10)
    |> Repo.all()
  end
end
