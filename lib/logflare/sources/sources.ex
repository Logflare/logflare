defmodule Logflare.Sources do
  @moduledoc """
  Sources-related context
  """
  alias Logflare.{Repo, Source, Tracker, Cluster}
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.Rule
  alias Logflare.User
  alias Logflare.SavedSearch
  require Logger
  @default_bucket_width 60

  @spec create_source(map(), User.t()) :: {:ok, Source.t()} | {:error, Ecto.Changeset.t()}
  def create_source(source_params, user) do
    user
    |> Ecto.build_assoc(:sources)
    |> Source.update_by_user_changeset(source_params)
    |> Repo.insert()
    |> case do
      {:ok, source} ->
        Source.Supervisor.new_source(source.token)

        {:ok, source}

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  def get(source_id) when is_atom(source_id) do
    get_by(token: source_id)
  end

  def get(source_id) do
    Source
    |> Repo.get(source_id)
  end

  def update_source(changeset) do
    Repo.update(changeset)
  end

  def update_source(source, attrs) do
    Source.changeset(source, attrs)
    |> Repo.update()
  end

  def get_by(kw) do
    Source
    |> Repo.get_by(kw)
  end

  def get_by_and_preload(kw) do
    Source
    |> Repo.get_by(kw)
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
      Tracker.Cache.get_cluster_rates(source.token).limiter_metrics
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

  def preload_defaults(source) do
    source
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
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
      saved_searches: from(SavedSearch) |> where([s], s.saved_by_user)
    )
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
    import Logflare.Source.Data
    alias Logflare.Logs.RejectedLogEvents
    alias Number.Delimit

    rates = Tracker.Cache.get_cluster_rates(token)
    buffer = Tracker.Cache.get_cluster_buffer(token)
    inserts = Tracker.Cache.get_cluster_inserts(token)
    inserts_string = Delimit.number_to_delimited(inserts)

    rejected_count = RejectedLogEvents.count(source)
    latest = get_latest_date(token)
    recent = get_ets_count(token)
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
    |> Repo.update()
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
end
