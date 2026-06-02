defmodule Logflare.Backends.UserMonitoring.IngestPipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message
  alias Logflare.Logs.OtelMetric
  alias Logflare.Logs.Processor
  alias Logflare.Logs.Raw
  alias Logflare.Sources
  alias Logflare.Users
  alias OtelMetricExporter.PullProducer

  def start_link(opts) do
    pull_interval = Keyword.get(opts, :pull_interval, 1_000)
    batch_timeout = Keyword.get(opts, :batch_timeout, 500)
    store_name = Keyword.fetch!(opts, :metric_store_name)

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {PullProducer, metric_store_name: store_name, pull_interval: pull_interval},
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        default: [concurrency: System.schedulers_online(), batch_size: 200, batch_timeout: batch_timeout]
      ]
    )
  end

  def transform(event, _opts) do
    %Message{data: event, acknowledger: {__MODULE__, :ack_id, :ack_data}}
  end

  def ack(_ack_ref, _successful, _failed), do: :ok

  @impl true
  def handle_message(_processor, message, _context) do
    Broadway.Message.update_data(message, &OtelMetric.handle_metric(&1, %{}, %{}))
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
      Processor.ingest(user_events, Raw, source)
    end
  end
end
