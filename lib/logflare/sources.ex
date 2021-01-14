defmodule Logflare.Sources do
  @moduledoc """
  Sources-related context
  """

  import Ecto.Query, only: [from: 2]
  use Logflare.Commons

  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias Logflare.PubSubRates
  alias Logflare.Cluster

  require Logger

  @default_bucket_width 60

  @spec create_source(map(), User.t()) :: {:ok, Source.t()} | {:error, Ecto.Changeset.t()}
  def create_source(source_params, user) do
    user
    |> Ecto.build_assoc(:sources)
    |> Source.update_by_user_changeset(source_params)
    |> RepoWithCache.insert()
    |> case do
      {:ok, source} ->
        {:ok, _source_schema} =
          create_source_schema(source, %{bigquery_schema: SchemaBuilder.initial_table_schema()})

        Source.Supervisor.start_source(source.token)

        {:ok, source}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  @spec get(atom | integer) :: Source.t() | nil
  def get(source_id) when is_atom(source_id) do
    get_by(token: source_id)
  end

  def get(source_id) when is_integer(source_id) do
    get_by(id: source_id)
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
    Source.changeset(source, attrs)
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

  @spec get_by(Keyword.t()) :: Source.t() | nil
  def get_by(kw) do
    RepoWithCache.get_by(Source, kw)
  end

  def get_by_id_and_preload(id) when is_integer(id) do
    get_by_and_preload(id: id)
  end

  def get_by_id_and_preload(token) when is_atom(token) do
    get_by_and_preload(token: token)
  end

  @spec get_by_and_preload(Keyword.t()) :: Source.t() | nil
  def get_by_and_preload(kw) do
    Source
    |> RepoWithCache.get_by(kw)
    |> case do
      nil ->
        nil

      s ->
        preload_defaults(s)
    end
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

  def get_bq_schema(%Source{} = source) do
    with %{schema: schema} <- Schema.get_state(source.token) do
      schema = SchemaUtils.deep_sort_by_fields_name(schema)
      {:ok, schema}
    else
      errtup -> errtup
    end
  end

  def put_bq_schema(source_token, bigquery_schema) when is_atom(source_token) do
    source = RepoWithCache.get_by(Source, token: source_token)
    source_schema = RepoWithCache.get_by(SourceSchema, source_id: source.id)
    update_source_schema(source_schema, %{bigquery_schema: bigquery_schema})
  end

  def preload_defaults(source) do
    source
    |> RepoWithCache.preload(:user)
    |> RepoWithCache.preload(:rules)
    |> refresh_source_metrics()
    |> maybe_compile_rule_regexes()
    |> put_bq_table_id()
  end

  def put_bq_table_data(source) do
    source
    |> put_bq_table_id()
    |> put_bq_table_schema()
    |> put_bq_table_typemap()
    |> put_bq_dataset_id()
  end

  def preload_saved_searches(source) do
    import Ecto.Query

    Repo.preload(
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

    %{source | metrics: metrics, has_rejected_events?: rejected_count > 0}
  end

  def put_schema_field_count(%Source{} = source) do
    new_metrics = %{source.metrics | fields: Source.Data.get_schema_field_count(source)}

    %{source | metrics: new_metrics}
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

  @spec put_bq_table_id(Source.t()) :: Source.t()
  def put_bq_table_id(%Source{} = source) do
    %{source | bq_table_id: Source.generate_bq_table_id(source)}
  end

  @spec put_bq_table_schema(Source.t()) :: Source.t()
  def put_bq_table_schema(%Source{} = source) do
    bq_table_schema =
      with {:ok, bq_table_schema} <- get_bq_schema(source) do
        bq_table_schema
      else
        {:error, error} -> raise(error)
      end

    %{source | bq_table_schema: bq_table_schema}
  end

  @spec put_bq_table_typemap(Source.t()) :: Source.t()
  def put_bq_table_typemap(%Source{} = source) do
    bq_table_typemap = SchemaUtils.to_typemap(source.bq_table_schema)
    %{source | bq_table_typemap: bq_table_typemap}
  end

  def put_bq_dataset_id(%Source{} = source) do
    %{bigquery_dataset_id: dataset_id} = GenUtils.get_bq_user_info(source.token)
    %{source | bq_dataset_id: dataset_id}
  end

  @doc """
  Returns the list of source_schemas.

  ## Examples

      iex> list_source_schemas()
      [%SourceSchema{}, ...]

  """
  def list_source_schemas do
    RepoWithCache.all(SourceSchema)
  end

  @doc """
  Gets a single source_schema.

  Raises `Ecto.NoResultsError` if the Source schema does not exist.

  ## Examples

      iex> get_source_schema!(123)
      %SourceSchema{}

      iex> get_source_schema!(456)
      ** (Ecto.NoResultsError)

  """
  def get_source_schema!(id), do: RepoWithCache.get!(SourceSchema, id)

  def get_source_schema_by(kv), do: SourceSchema |> RepoWithCache.get_by(kv)

  @doc """
  Creates a source_schema.

  ## Examples

      iex> create_source_schema(%{field: value})
      {:ok, %SourceSchema{}}

      iex> create_source_schema(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_source_schema(source, attrs \\ %{}) do
    source
    |> Ecto.build_assoc(:source_schema)
    |> SourceSchema.changeset(attrs)
    |> RepoWithCache.insert()
  end

  def find_or_create_source_schema(source) do
    default = %{bigquery_schema: SchemaBuilder.initial_table_schema()}

    case get_source_schema_by(source_id: source.id) do
      nil -> create_source_schema(source, default)
      schema -> {:ok, schema}
    end
  end

  def create_or_update_source_schema(source, attrs) do
    case get_source_schema_by(source_id: source.id) do
      nil -> create_source_schema(source, attrs)
      source_schema -> update_source_schema(source_schema, attrs)
    end
  end

  @doc """
  Updates a source_schema.

  ## Examples

      iex> update_source_schema(source_schema, %{field: new_value})
      {:ok, %SourceSchema{}}

      iex> update_source_schema(source_schema, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_source_schema(%SourceSchema{} = source_schema, attrs) do
    source_schema
    |> SourceSchema.changeset(attrs)
    |> RepoWithCache.update()
  end

  @doc """
  Deletes a source_schema.

  ## Examples

      iex> delete_source_schema(source_schema)
      {:ok, %SourceSchema{}}

      iex> delete_source_schema(source_schema)
      {:error, %Ecto.Changeset{}}

  """
  def delete_source_schema(%SourceSchema{} = source_schema) do
    RepoWithCache.delete(source_schema)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking source_schema changes.

  ## Examples

      iex> change_source_schema(source_schema)
      %Ecto.Changeset{data: %SourceSchema{}}

  """
  def change_source_schema(%SourceSchema{} = source_schema, attrs \\ %{}) do
    SourceSchema.changeset(source_schema, attrs)
  end

  def count_for_billing(sources) do
    count = Enum.count(sources)

    if count == 0, do: 1, else: count
  end

  @spec get_source_for_lv_param(binary | integer) :: Logflare.Source.t()
  def get_source_for_lv_param(source_id) when is_binary(source_id) or is_integer(source_id) do
    get_by_and_preload(id: source_id)
    |> preload_saved_searches()
    |> put_bq_table_data()
  end

  @spec get_table_partition_type(Source.t()) :: :timestamp | :pseudo
  def get_table_partition_type(%Source{} = source) do
    case source.bq_table_partition_type do
      nil -> :pseudo
      x -> x
    end
  end
end
