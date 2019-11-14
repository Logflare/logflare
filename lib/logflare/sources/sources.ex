defmodule Logflare.Sources do
  @moduledoc """
  Sources-related context
  """
  alias Logflare.{Repo, Source, Tracker, Cluster}
  require Logger

  @default_bucket_width 60

  def get_by(kw) do
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
    cluster_size = Cluster.Utils.node_list_all() |> Enum.count()
    cluster_metrics = Tracker.Cache.get_cluster_rates(source.token).limiter_metrics
    node_metrics = get_node_rate_limiter_metrics(source, bucket: :default)
    failsafe = node_rate_limiter_failsafe(node_metrics, cluster_size)

    if source.api_quota * @default_bucket_width < node_metrics.sum * cluster_size do
      failsafe
    else
      cluster_metrics
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
    {:ok, table} = Logflare.Google.BigQuery.get_table(source.token)
    {:ok, table.schema}
  end

  def preload_defaults(source) do
    source
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
    |> Repo.preload(:saved_searches)
    |> refresh_source_metrics()
    |> maybe_compile_rule_regexes()
    |> Source.put_bq_table_id()
  end

  # """
  # Compiles regex_struct if it's not present in the source rules.
  # By setting regex_struct to nil if invalid, prevents malformed regex matching during log ingest.
  # """
  defp maybe_compile_rule_regexes(%{rules: rules} = source) do
    rules =
      for rule <- rules do
        if rule.regex_struct do
          rule
        else
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

  def put_schema_field_count(source) do
    new_metrics = %{source.metrics | fields: Source.Data.get_schema_field_count(source)}

    %{source | metrics: new_metrics}
  end

  def valid_source_token_param?(string) when is_binary(string) do
    case String.length(string) === 36 && Ecto.UUID.cast(string) do
      {:ok, _} -> true
      false -> false
      :error -> false
    end
  end

  def valid_source_token_param?(_), do: false

  def delete_slack_hook_url(source) do
    source
    |> Source.changeset(%{slack_hook_url: nil})
    |> Repo.update()
  end
end
