defmodule Logflare.Sources do
  @moduledoc """
  Sources-related context
  """

  import Ecto.Query, only: [from: 2]
  use Logflare.Commons

  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.PubSubRates
  alias Logflare.Cluster

  require Logger

  @default_bucket_width 60

  @spec create_source(map(), User.t()) :: {:ok, Source.t()} | {:error, Ecto.Changeset.t()}
  def create_source(source_params, %User{} = user) do
    user
    |> Ecto.build_assoc(:sources)
    |> Source.update_by_user_changeset(source_params)
    |> RepoWithCache.insert()
    |> case do
      {:ok, source} ->
        {:ok, _source_schema} =
          SourceSchemas.create_source_schema_for_source(
            %{bigquery_schema: SchemaBuilder.initial_table_schema()},
            source
          )

        Source.Supervisor.start_source(source.token)

        {:ok, source}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  @spec get_source(atom | integer) :: Source.t() | nil
  def get_source(source_id) when is_atom(source_id) do
    get_source_by(token: source_id)
  end

  def get_source(source_id) when is_integer(source_id) do
    get_source_by(id: source_id)
  end

  @spec get_source!(atom | integer) :: Source.t()
  def get_source!(source_id) when is_integer(source_id) do
    get_source_by!(id: source_id)
  end

  def get_source!(source_id) when is_atom(source_id) do
    get_source_by!(token: source_id)
  end

  def get_sources_by_user(%User{id: id}) do
    from(s in Source,
      where: s.user_id == ^id,
      select: s
    )
    |> RepoWithCache.all()
  end

  def update_source(changeset) do
    RepoWithCache.update(changeset)
  end

  def update_source(source, attrs) do
    source
    |> Source.changeset(attrs)
    |> RepoWithCache.update()
  end

  def update_source_by_user(_source, _plan, %{"notifications_every" => ""}) do
    {:error, :select_frequency}
  end

  def update_source_by_user(source, plan, %{"notifications_every" => freq} = attrs) do
    freq = String.to_integer(freq)
    limit = plan.limit_alert_freq

    if freq < limit do
      {:error, :upgrade}
    else
      Source.update_by_user_changeset(source, attrs)
      |> RepoWithCache.update()
      |> case do
        {:ok, source} = response ->
          Source.Supervisor.reset_source(source.token)

          response

        response ->
          response
      end
    end
  end

  @spec get_source_by(Keyword.t()) :: Source.t() | nil
  def get_source_by(kw) do
    RepoWithCache.get_by(Source, kw)
  end

  @spec get_source_by!(Keyword.t()) :: Source.t()
  def get_source_by!(kw) do
    RepoWithCache.get_by!(Source, kw)
  end

  def get_by_id_and_preload(id) when is_integer(id) do
    get_by_and_preload(id: id)
  end

  def get_by_id_and_preload(token) when is_atom(token) do
    get_by_and_preload(token: token)
  end

  @spec get_by_and_preload(Keyword.t()) :: Source.t() | nil
  def get_by_and_preload(kw) do
    get_source_by(kw)
    |> preload_defaults()
  end

  def get_rate_limiter_metrics(source, bucket: :default) do
    cluster_size = Cluster.Utils.cluster_size()
    node_metrics = get_node_rate_limiter_metrics(source, bucket: :default)

    if source.api_quota * @default_bucket_width < node_metrics.sum * cluster_size do
      node_rate_limiter_failsafe(node_metrics, cluster_size)
    else
      PubSubRates.Cache.get_cluster_rates(source.token).limiter_metrics
    end
  end

  def delete_source(source) do
    case RepoWithCache.delete(source) do
      {:ok, response} ->
        {:ok, response}

      {:error, response} ->
        {:error, response}
    end
  end

  def node_rate_limiter_failsafe(node_metrics, cluster_size) do
    %{
      node_metrics
      | average: node_metrics.average * cluster_size,
        sum: node_metrics.sum * cluster_size
    }
  end

  def get_node_rate_limiter_metrics(source, bucket: :default) do
    source.token
    |> Source.RateCounterServer.get_data_from_ets()
    |> Map.get(:buckets)
    |> Map.get(@default_bucket_width)
    |> Map.drop([:queue])
  end

  @spec preload_sources_for_dashboard(Source.t() | [Source.t()]) :: Source.t() | [Source.t()]
  def preload_sources_for_dashboard(sources) when is_list(sources) do
    sources
    |> Enum.map(&preload_sources_for_dashboard/1)
    |> Enum.sort_by(& &1.name, &<=/2)
    |> Enum.sort_by(& &1.favorite, &>=/2)
  end

  def preload_sources_for_dashboard(%Source{} = source) do
    source
    |> RepoWithCache.preload(:rules)
    |> RepoWithCache.preload(:source_schema)
    |> Sources.preload_saved_by_user_searches()
    |> Sources.refresh_source_metrics()
  end

  def preload_defaults(nil), do: nil

  def preload_defaults(source) do
    source
    |> RepoWithCache.preload(:user)
    |> RepoWithCache.preload(:rules)
    |> RepoWithCache.preload(:source_schema)
    |> maybe_compile_rule_regexes()
  end

  def preload_saved_by_user_searches(source) do
    import Ecto.Query

    RepoWithCache.preload(
      source,
      saved_searches: from(SavedSearch) |> where([s], s.saved_by_user == true)
    )
  end

  def preload_source_schema(source) do
    RepoWithCache.preload(source, :source_schema)
  end

  # """
  # Compiles regex_struct if it's not present in the source rules.
  # By setting regex_struct to nil if invalid, prevents malformed regex matching during log ingest.
  # """
  defp maybe_compile_rule_regexes(%{rules: rules} = source) do
    rules =
      for rule <- rules do
        case rule do
          %Rule{lql_filters: lql_filters} when length(lql_filters) >= 1 ->
            rule

          %Rule{regex_struct: rs} when not is_nil(rs) ->
            rule

          %Rule{regex: regex} when not is_nil(regex) ->
            regex_struct =
              case Regex.compile(rule.regex) do
                {:ok, regex} ->
                  regex

                {:error, _} ->
                  Logger.error(
                    "Rule #{rule.id} for #{source.token} is invalid. Regex string:#{rule.regex}"
                  )

                  nil
              end

            %{rule | regex_struct: regex_struct}
        end
      end

    %{source | rules: rules}
  end

  def refresh_source_metrics(%Source{token: token} = source) do
    alias Logflare.Logs.RejectedLogEvents
    alias Number.Delimit
    alias Logflare.Source.RecentLogsServer, as: RLS

    rates = PubSubRates.Cache.get_cluster_rates(token)
    buffer = PubSubRates.Cache.get_cluster_buffers(token)
    inserts = PubSubRates.Cache.get_cluster_inserts(token)
    inserts_string = Delimit.number_to_delimited(inserts)

    rejected_count = RejectedLogEvents.count(source)
    latest = RLS.get_latest_date(token)
    recent = Enum.count(RLS.list(token))
    fields = 0

    metrics = %Source.Metrics{
      rate: rates.last_rate,
      latest: latest,
      avg: rates.average_rate,
      max: rates.max_rate,
      buffer: buffer,
      inserts_string: inserts_string,
      inserts: inserts,
      recent: recent,
      rejected: rejected_count,
      fields: fields
    }

    %{source | metrics: metrics, has_rejected_events: rejected_count > 0}
  end

  def valid_source_token_param?(string) when is_binary(string) do
    case String.length(string) === 36 && Ecto.UUID.cast(string) do
      {:ok, _} -> true
      _ -> false
    end
  end

  def valid_source_token_param?(_), do: false

  def delete_slack_hook_url(source) do
    source
    |> Source.changeset(%{slack_hook_url: nil})
    |> RepoWithCache.update()
  end

  def count_for_billing(sources) do
    count = Enum.count(sources)

    if count == 0, do: 1, else: count
  end

  @spec get_source_for_lv_param(binary | integer) :: Logflare.Source.t()
  def get_source_for_lv_param(source_id) when is_binary(source_id) or is_integer(source_id) do
    [id: source_id]
    |> get_by_and_preload()
    |> preload_saved_by_user_searches()
  end

  @spec get_table_partition_type(Source.t()) :: :timestamp | :pseudo
  def get_table_partition_type(%Source{} = source) do
    case source.bq_table_partition_type do
      nil -> :pseudo
      x -> x
    end
  end
end
