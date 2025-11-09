defmodule Logflare.Sources do
  @moduledoc """
  Sources-related context
  """

  import Ecto.Query

  alias Logflare.Backends
  alias Logflare.Billing
  alias Logflare.Billing.Plan
  alias Logflare.Cluster
  alias Logflare.Google.BigQuery
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Logs.RejectedLogEvents
  alias Logflare.PubSubRates
  alias Logflare.Repo
  alias Logflare.SavedSearch
  alias Logflare.SingleTenant
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.User
  alias Logflare.Users
  alias Number.Delimit

  require Logger

  @default_bucket_width 60

  @spec count_sources_by_user(User.t() | pos_integer()) :: non_neg_integer()
  def count_sources_by_user(%User{id: user_id}), do: count_sources_by_user(user_id)

  def count_sources_by_user(user_id) do
    from(s in Source, where: s.user_id == ^user_id)
    |> Repo.aggregate(:count)
  end

  @spec list_sources_by_user(User.t() | pos_integer()) :: [Source.t()]
  def list_sources_by_user(%User{id: user_id}), do: list_sources_by_user(user_id)

  def list_sources_by_user(user_id) when is_integer(user_id) do
    from(s in Source, where: s.user_id == ^user_id)
    |> Repo.all()
    |> Enum.map(&put_retention_days/1)
  end

  @doc """
  Lists sources based on provided filters.
  """
  @spec list_sources(Keyword.t()) :: [Source.t()]
  def list_sources(filters) when is_list(filters) do
    filters
    |> Enum.reduce(from(s in Source), fn
      {:backend_id, backend_id}, q when is_integer(backend_id) ->
        from(s in q,
          join: sb in "sources_backends",
          on: sb.source_id == s.id,
          where: sb.backend_id == ^backend_id
        )

      {:user_id, user_id}, q when is_integer(user_id) ->
        where(q, [s], s.user_id == ^user_id)

      {:default_ingest_backend_enabled?, enabled}, q when is_boolean(enabled) ->
        where(q, [s], s.default_ingest_backend_enabled? == ^enabled)

      _, q ->
        q
    end)
    |> Repo.all()
    |> Enum.map(&put_retention_days/1)
  end

  @spec create_source(map(), User.t()) :: {:ok, Source.t()} | {:error, Ecto.Changeset.t()}
  def create_source(source_params, user) do
    source_params =
      source_params
      |> Enum.map(fn {k, v} -> {to_string(k), v} end)
      |> Map.new()

    with {:ok, source} <-
           user
           |> Ecto.build_assoc(:sources)
           |> Source.update_by_user_changeset(source_params)
           |> Repo.insert() do
      if !SingleTenant.postgres_backend?() do
        create_big_query_schema_and_start_source(source)
      end

      updated =
        source
        |> put_retention_days()

      {:ok, updated}
    end
  end

  @doc """
  To be replaced by BigQuery v2 adaptor
  """
  def create_big_query_schema_and_start_source(source) do
    init_schema = SchemaBuilder.initial_table_schema()

    {:ok, _source_schema} =
      SourceSchemas.create_source_schema(source, %{
        bigquery_schema: init_schema,
        schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(init_schema)
      })

    Source.Supervisor.start_source(source.token)

    {:ok, source}
  end

  @doc """
  Retrieves a source by its uuid token
  """
  @spec get_source_by_token(atom | String.t()) :: Source.t() | nil
  def get_source_by_token(source_token) when is_atom(source_token) or is_binary(source_token) do
    get_by(token: source_token)
  end

  @doc """
  Retrieves a source by keyword, with `{:error, :not_found}` if not found.
  """
  @spec fetch_source_by(keyword()) :: {:ok, Source.t()} | {:error, :not_found}
  def fetch_source_by(kw) do
    source = get_by(kw)

    if source do
      {:ok, source}
    else
      {:error, :not_found}
    end
  end

  @doc """
  deprecated, use get_source_by_token/1 instead
  """
  @spec get(atom | integer) :: Source.t() | nil
  def get(source_token) when is_atom(source_token) do
    get_source_by_token(source_token)
  end

  def get(source_id) when is_integer(source_id) or is_binary(source_id) do
    Repo.get(Source, source_id)
    |> put_retention_days()
  end

  def update_source(source) do
    Repo.update(source)
  end

  def update_source(source, attrs) do
    source
    |> Source.changeset(attrs)
    |> Repo.update()
    |> post_update(source)
  end

  defp post_update({:ok, updated}, source) do
    # only update the default backend
    source = put_retention_days(source)
    updated = put_retention_days(updated)

    if source.retention_days != updated.retention_days and not SingleTenant.postgres_backend?() do
      user = Users.Cache.get(updated.user_id)

      BigQuery.patch_table_ttl(
        updated.token,
        updated.retention_days * 86_400_000,
        user.bigquery_dataset_id,
        user.bigquery_project_id
      )
    end

    if source.bigquery_clustering_fields != updated.bigquery_clustering_fields and
         not SingleTenant.postgres_backend?() do
      user = Users.Cache.get(updated.user_id)

      fields = String.split(updated.bigquery_clustering_fields || "", ",") ++ ["timestamp"]

      BigQuery.patch_table_clustering(
        updated.token,
        fields,
        user.bigquery_dataset_id,
        user.bigquery_project_id
      )
    end

    {:ok, updated}
  end

  defp post_update(res, _prev), do: res

  def update_source_by_user(source, attrs) do
    source
    |> Source.update_by_user_changeset(attrs)
    |> Repo.update()
    |> post_update(source)
  end

  def update_source_by_user(_source, _plan, %{"notifications_every" => ""}) do
    {:error, :select_frequency}
  end

  def update_source_by_user(source, plan, %{"notifications_every" => freq} = attrs) do
    freq = String.to_integer(freq)
    limit = plan.limit_alert_freq

    case freq < limit do
      true ->
        {:error, :upgrade}

      false ->
        update =
          source
          |> Source.update_by_user_changeset(attrs)
          |> Repo.update()

        case update do
          {:ok, source} = response ->
            Source.Supervisor.reset_source(source.token)

            response

          response ->
            response
        end
    end
  end

  @spec get_by(Keyword.t()) :: Source.t() | nil
  def get_by(kw) do
    Repo.get_by(Source, kw)
    |> put_retention_days()
  end

  @spec get_by_and_preload(Keyword.t()) :: Source.t() | nil
  def get_by_and_preload(kw) do
    Source
    |> Repo.get_by(kw)
    |> then(fn
      nil -> nil
      s -> preload_defaults(s)
    end)
    |> put_retention_days()
  end

  @spec get_by_and_preload(Keyword.t(), Keyword.t()) :: Source.t() | nil
  def get_by_and_preload(kw, preloads) do
    Source
    |> Repo.get_by(kw)
    |> then(fn
      nil -> nil
      s -> Repo.preload(s, preloads)
    end)
    |> put_retention_days()
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
    Repo.delete(source)
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

  def get_bq_schema(%Source{} = source) do
    case SourceSchemas.Cache.get_source_schema_by(source_id: source.id) do
      nil ->
        {:ok, SchemaBuilder.initial_table_schema()}

      %_{bigquery_schema: schema} ->
        schema = SchemaUtils.deep_sort_by_fields_name(schema)
        {:ok, schema}
    end
  end

  def get_by_and_preload_rules(kv) do
    case get_by(kv) do
      nil -> nil
      source -> Repo.preload(source, :rules)
    end
  end

  @spec preload_defaults(Source.t()) :: Source.t()
  def preload_defaults(source) do
    source
    |> Repo.preload([:user, :rules, :backends])
    |> refresh_source_metrics()
    |> put_bq_table_id()
  end

  def preload_rules(source) do
    source
    |> Repo.preload([:rules])
  end

  @spec preload_saved_searches(Source.t() | [Source.t()], Keyword.t()) :: Source.t()
  def preload_saved_searches(source, opts \\ []) do
    import Ecto.Query

    Repo.preload(
      source,
      [saved_searches: from(s in SavedSearch, where: s.saved_by_user)],
      opts
    )
  end

  def preload_source_schema(source) do
    Repo.preload(source, :source_schema)
  end

  def preload_backends(source) do
    Repo.preload(source, :backends)
  end

  def get_source_metrics_for_ingest(%Source{token: token}),
    do: get_source_metrics_for_ingest(token)

  def get_source_metrics_for_ingest(source_token) when is_atom(source_token) do
    rates = PubSubRates.Cache.get_cluster_rates(source_token)

    metrics = %Source.Metrics{
      avg: rates.average_rate
    }

    metrics
  end

  def refresh_source_metrics_for_ingest(nil), do: nil

  def refresh_source_metrics_for_ingest(%Source{token: token} = source) do
    rates = PubSubRates.Cache.get_cluster_rates(token)

    metrics = %Source.Metrics{
      avg: rates.average_rate
    }

    %{source | metrics: metrics}
  end

  def refresh_source_metrics(nil), do: nil

  def refresh_source_metrics(%Source{token: token} = source) do
    rates = PubSubRates.Cache.get_cluster_rates(token)
    buffer = PubSubRates.Cache.get_cluster_buffers(source.id)
    inserts = PubSubRates.Cache.get_cluster_inserts(token)
    inserts_string = Delimit.number_to_delimited(inserts)

    rejected_count = RejectedLogEvents.count(source)
    latest = Backends.fetch_latest_timestamp(source)
    fields = 0

    metrics = %Source.Metrics{
      rate: rates.last_rate,
      latest: latest,
      avg: rates.average_rate,
      max: rates.max_rate,
      buffer: buffer,
      inserts_string: inserts_string,
      inserts: inserts,
      rejected: rejected_count,
      fields: fields
    }

    %{source | metrics: metrics, has_rejected_events: rejected_count > 0}
  end

  def put_schema_field_count(%Source{} = source) do
    new_metrics = %{source.metrics | fields: Source.Data.get_schema_field_count(source)}

    %{source | metrics: new_metrics}
  end

  def valid_source_token_param?(string) do
    match?({:ok, _}, Ecto.UUID.dump(string))
  end

  def delete_slack_hook_url(source) do
    source
    |> Source.changeset(%{slack_hook_url: nil})
    |> Repo.update()
  end

  @spec put_bq_table_id(Source.t()) :: Source.t()
  def put_bq_table_id(%Source{} = source) do
    %{source | bq_table_id: Source.generate_bq_table_id(source)}
  end

  def put_bq_dataset_id(%Source{} = source) do
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)
    %{source | bq_dataset_id: dataset_id}
  end

  def count_for_billing(sources) do
    count = Enum.count(sources)

    if count == 0, do: 1, else: count
  end

  @spec get_source_for_lv_param(binary | integer) :: Logflare.Sources.Source.t()
  def get_source_for_lv_param(source_id) when is_binary(source_id) or is_integer(source_id) do
    get_by_and_preload(id: source_id)
    |> preload_saved_searches()
    |> put_bq_table_id()
    |> put_bq_dataset_id()
  end

  @spec get_table_partition_type(Source.t()) :: :timestamp | :pseudo
  def get_table_partition_type(%Source{} = source) do
    case source.bq_table_partition_type do
      nil -> :pseudo
      x -> x
    end
  end

  @spec preload_for_dashboard(list(Source.t())) :: list(Source.t())
  def preload_for_dashboard(sources) do
    sources
    |> Enum.map(&preload_defaults/1)
    |> Enum.map(&preload_saved_searches/1)
    |> Enum.map(&put_schema_field_count/1)
    |> Enum.sort_by(&{!&1.favorite, &1.name})
  end

  @doc "Checks if all ETS tables used for source ingestion are started"
  @spec ingest_ets_tables_started?() :: boolean()
  def ingest_ets_tables_started? do
    :ets.whereis(:rate_counters) != :undefined and :ets.whereis(:table_counters) != :undefined
  end

  @spec put_retention_days(Source.t() | nil) :: Source.t() | nil
  def put_retention_days(%Source{} = source) do
    user = Users.Cache.get(source.user_id)
    plan = Billing.Cache.get_plan_by_user(user)
    %{source | retention_days: source_ttl_to_days(source, plan)}
  end

  def put_retention_days(source), do: source

  @doc """
  Formats a source TTL to the specified unit
  """
  @spec source_ttl_to_days(Source.t(), Plan.t()) :: integer()
  def source_ttl_to_days(%Source{bigquery_table_ttl: ttl}, _plan)
      when ttl >= 0 and ttl != nil do
    round(ttl)
  end

  # fallback to plan value or default init value
  # use min to avoid misrepresenting what user should see, in cases where actual is more than plan.
  def source_ttl_to_days(_source, %Plan{limit_source_ttl: ttl}) do
    min(
      round(GenUtils.default_table_ttl_days()),
      round(ttl / :timer.hours(24))
    )
  end

  @doc """
  Shuts down idle source supervisors across the cluster.

  This function checks all running SourceSup processes and shuts down those that are idle
  (no ingest activity and no pending items in their backend queues).
  """
  @spec shutdown_idle_sources() :: :ok
  def shutdown_idle_sources do
    Sources.Cache
    |> Cachex.stream!()
    |> Stream.filter(fn
      # Filter for get_by calls that return a Source struct with an id
      {:entry, {:get_by, _args}, {:cached, %Source{id: id} = source}, _ts, _ttl}
      when is_integer(id) ->
        Backends.source_sup_started?(id) and source_idle?(source)

      _val ->
        false
    end)
    |> Stream.map(fn {:entry, {:get_by, _}, {:cached, %Source{id: id}}, _ts, _ttl} -> id end)
    |> Stream.uniq()
    |> Enum.to_list()
    |> then(fn
      [] ->
        []

      source_ids ->
        Logger.debug("Shutting down #{Enum.count(source_ids)} idle sources")

        source_ids
    end)
    |> Task.async_stream(
      fn source_id ->
        source = Sources.Cache.get_by_id(source_id)
        Source.Supervisor.stop_source_local(source)
      end,
      max_concurrency: 3,
      timeout: 30_000,
      on_timeout: :kill_task
    )
    |> Stream.run()

    :ok
  end

  defp source_idle?(source) do
    metrics = get_source_metrics_for_ingest(source)
    total_pending = calculate_total_pending(source)
    metrics.avg == 0 and total_pending == 0
  end

  @spec calculate_total_pending(Source.t()) :: non_neg_integer()
  defp calculate_total_pending(%Source{id: source_id} = source) do
    # Get all backends for this source, including default backend
    user = Users.Cache.get(source.user_id)
    default_backend = Backends.get_default_backend(user)

    ingest_backends = Backends.Cache.list_backends(source_id: source.id)

    rules_backends =
      Backends.Cache.list_backends(rules_source_id: source.id)
      |> Enum.map(&%{&1 | register_for_ingest: false})

    all_backends = [default_backend | ingest_backends] ++ rules_backends

    # Sum pending items across all backends
    all_backends
    |> Enum.reduce(0, fn backend, acc ->
      case Backends.IngestEventQueue.total_pending({source_id, backend.id}) do
        {:error, :not_initialized} -> acc
        count -> acc + count
      end
    end)
  end
end
