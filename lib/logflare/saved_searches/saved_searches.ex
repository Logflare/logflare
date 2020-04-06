defmodule Logflare.SavedSearches do
  @moduledoc false
  import Ecto.Query
  alias Logflare.{SavedSearch, Repo}
  alias Logflare.{Sources, Source}
  alias Logflare.Lql
  alias Logflare.Lql.{FilterRule, ChartRule}
  alias Logflare.SavedSearchCounter
  alias Logflare.DateTimeUtils
  require Logger

  def get(id) do
    Repo.get(SavedSearch, id)
  end

  @spec insert(map(), Source.t()) :: {:ok, SavedSearch} | {:error, term}
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
      insert(%{querystring: querystring, lql_rules: lql_rules, saved_by_user: true}, source)
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
      non_tailing_count: 0
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

  def update_lql_rules_where_nil() do
    Logger.info("Updating saved searches, populating lql rules where nil...")

    searches =
      SavedSearch
      |> where([s], is_nil(s.lql_filters) or is_nil(s.lql_charts))
      |> Repo.all()

    for search <- searches do
      source =
        search.source_id
        |> Sources.get()
        |> Sources.put_bq_table_data()

      with {:ok, lql_rules} <- Lql.decode(search.querystring, source.bq_table_schema) do
        lql_filters = Lql.Utils.get_filter_rules(lql_rules)
        lql_charts = Lql.Utils.get_chart_rules(lql_rules)

        search
        |> SavedSearch.changeset(%{lql_filters: lql_filters, lql_charts: lql_charts})
        |> Repo.update()
        |> case do
          {:ok, search} ->
            Logger.info(
              "Saved search #{search.id} for source #{search.source_id} was successfully updated with LQL filters and charts."
            )

          {:error, changeset} ->
            Logger.error(
              "Saved search #{search.id} for source #{search.source_id} failed to update LQL filters and charts, Repo update error: #{
                inspect(changeset.errors)
              }"
            )
        end
      else
        {:error, error} ->
          Logger.error(
            "Saved search #{search.id} for source #{search.source_id} failed to upgrade to new LQL filters format, LQL decoding error: #{
              inspect(error)
            }"
          )
      end
    end
  end
end
