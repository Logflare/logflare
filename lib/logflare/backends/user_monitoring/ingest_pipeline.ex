defmodule Logflare.Backends.UserMonitoring.IngestPipeline do
  use Broadway

  alias Broadway.Message
  alias Logflare.Logs.OtelMetric
  alias Logflare.Logs.Processor
  alias Logflare.Sources
  alias Logflare.Users
  alias OtelMetricExporter.PullProducer

  def start_link(_opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {PullProducer, metric_store_name: :user_metrics_exporter, pull_interval: 1000},
        concurrency: 2,
        transformer: {__MODULE__, :transform, []}
      ],
      processors: [
        default: [concurrency: System.schedulers_online()]
      ],
      batchers: [
        default: [
          concurrency: System.schedulers_online(),
          batch_size: 200,
          batch_timeout: 500
        ]
      ]
    )
  end

  def transform(event, _opts) do
    %Message{data: event, acknowledger: Broadway.NoopAcknowledger.init()}
  end

  @impl true
  def handle_message(_processor, msg, _ctx) do
    Broadway.Message.update_data(msg, &OtelMetric.handle_metric(&1, %{}, %{}))
  end

  @impl true
  def handle_batch(:default, messages, _batch_info, _ctx) do
    messages
    |> Enum.flat_map(& &1.data)
    |> Enum.group_by(fn event -> Users.get_related_user_id(Map.get(event, "attributes")) end)
    |> ingest_grouped_metrics()

    messages
  end

  def ingest_grouped_metrics(grouped_events)
      when is_list(grouped_events) or is_map(grouped_events) do
    Enum.each(grouped_events, fn {user_id, user_events} ->
      ingest_grouped_metrics({user_id, user_events})
    end)
  end

  def ingest_grouped_metrics({user_id, user_events}) do
    with %Sources.Source{} = source <-
           Sources.Cache.get_by(user_id: user_id, system_source_type: :metrics) do
      Processor.ingest(user_events, Logflare.Logs.Raw, source)
    end
  end
end
