defmodule Logflare.Sources do
  alias Logflare.{Repo, Source}
  alias Logflare.Source.RateCounterServer, as: SRC

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

  def get_metrics(source, bucket: :default) do
    # Source bucket metrics
    SRC.get_metrics(source.token, :default)
  end

  def get_api_rate(source, bucket: :default) do
    SRC.get_avg_rate(source.token)
  end

  def get_bq_schema(%Source{} = source) do
    Logflare.Google.BigQuery.get_table(source.token)
  end

  def preload_defaults(source) do
    source
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
    |> refresh_source_metrics()
    |> Source.put_bq_table_id()
  end

  def refresh_source_metrics(%Source{token: token} = source) do
    import Logflare.Source.Data
    alias Logflare.Logs.RejectedLogEvents
    alias Number.Delimit

    rejected_count = RejectedLogEvents.count(source)
    inserts = get_total_inserts(token)
    buffer = get_buffer(token)
    max = get_max_rate(token)
    avg = get_avg_rate(token)
    latest = get_latest_date(token)
    rate = get_rate(token)
    recent = get_ets_count(token)

    metrics = %Source.Metrics{
      rate: rate,
      rate_int: rate,
      latest: latest,
      avg: avg,
      avg_int: avg,
      max: max,
      max_int: max,
      buffer: buffer,
      buffer_int: buffer,
      inserts: inserts,
      inserts_int: inserts,
      recent: recent,
      recent_int: recent,
      rejected: rejected_count,
      rejected_int: rejected_count
    }

    metrics =
      Enum.reduce(~w[rate avg max buffer inserts rejected recent]a, metrics, fn key, ms ->
        Map.update!(ms, key, &Delimit.number_to_delimited/1)
      end)

    %{source | metrics: metrics, has_rejected_events?: rejected_count > 0}
  end
end
