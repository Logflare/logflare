defmodule Logflare.Sources do
  alias Logflare.{Repo, Source}
  alias Logflare.SourceRateCounter, as: SRC

  def get_by(kw) do
    Source
    |> Repo.get_by(kw)
    |> preload_defaults()
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
    import Logflare.SourceData
    alias Logflare.Logs.RejectedEvents
    alias Number.Delimit
    rejected_count = RejectedEvents.get_by_source(source)

    metrics =
      %Source.Metrics{
        rate: get_rate(token),
        latest: get_latest_date(token),
        avg: get_avg_rate(token),
        max: get_max_rate(token),
        buffer: get_buffer(token),
        inserts: get_total_inserts(token),
        rejected: rejected_count
      }
      |> Map.from_struct()
      |> Enum.map(fn
        {k, v} when k in ~w[rate latest avg max buffer inserts]a ->
          {k, Delimit.number_to_delimited(v)}

        x ->
          x
      end)
      |> Map.new()

    %{source | metrics: metrics, has_rejected_events?: rejected_count > 0}
  end
end
