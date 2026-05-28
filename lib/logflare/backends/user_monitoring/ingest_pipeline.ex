defmodule Logflare.Backends.UserMonitoring.IngestPipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message
  alias Logflare.Logs.OtelMetric
  alias Logflare.Logs.Processor
  alias Logflare.Logs.Raw
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.UserMetrics.PullProducer

  def start_link(opts) do
    pull_interval = Keyword.get(opts, :pull_interval, 1_000)
    batch_timeout = Keyword.get(opts, :batch_timeout, 5_000)
    store_name = Keyword.fetch!(opts, :metric_store_name)

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 10_000],
      producer: [
        module: {PullProducer, metric_store_name: store_name, pull_interval: pull_interval},
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 1]
      ],
      batchers: [
        ingest: [concurrency: 2, batch_size: 50, batch_timeout: batch_timeout]
      ]
    )
  end

  def transform(event, _opts) do
    %Message{data: event, acknowledger: {__MODULE__, :ack_id, :ack_data}}
  end

  def ack(_ack_ref, _successful, _failed), do: :ok

  @impl true
  def handle_message(_processor, message, _context) do
    Message.put_batcher(message, :ingest)
  end

  @impl true
  def handle_batch(:ingest, messages, _batch_info, _context) do
    messages
    |> Enum.flat_map(fn msg ->
      OtelMetric.handle_metric(msg.data, %{}, %{})
    end)
    |> Enum.reduce(%{}, fn event, acc ->
      user_id = Users.get_related_user_id(Map.get(event, "attributes"))
      Map.update(acc, user_id, [event], &[event | &1])
    end)
    |> Enum.each(fn {user_id, user_events} ->
      with %Sources.Source{} = source <-
             Sources.Cache.get_by(user_id: user_id, system_source_type: :metrics) do
        Processor.ingest(user_events, Raw, source)
      end
    end)

    messages
  end
end
