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

  def preload_defaults(source) do
    source
    |> Repo.preload(:user)
    |> Repo.preload(:rules)
    |> refresh_source_metrics()
  end

  def refresh_source_metrics(%Source{token: token} = source) do
    import Logflare.Source.Data
    alias Logflare.Logs.RejectedEvents
    alias Number.Delimit
    rejected_count = RejectedEvents.get_by_source(source)

    metrics = %Source.Metrics{
      rate: get_rate(token),
      latest: get_latest_date(token),
      avg: get_avg_rate(token),
      max: get_max_rate(token),
      buffer: get_buffer(token),
      inserts: get_total_inserts(token),
      rejected: rejected_count
    }

    metrics =
      Enum.reduce(~w[rate avg max buffer inserts]a, metrics, fn key, ms ->
        Map.update!(ms, key, &Delimit.number_to_delimited/1)
      end)

    %{source | metrics: metrics, has_rejected_events?: rejected_count > 0}
  end
end
