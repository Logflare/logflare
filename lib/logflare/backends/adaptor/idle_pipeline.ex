defmodule Logflare.Backends.Adaptor.IdlePipeline do
  @moduledoc """
  Builds single-shard DynamicPipeline options for adaptors that can stop when idle.
  """

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources.Source

  @spec enabled?(:webhook | :http_based) :: boolean()
  def enabled?(group) do
    group in Application.get_env(:logflare, :scale_to_zero_adaptors, [])
  end

  @spec dynamic_pipeline_opts(Source.t(), Backend.t(), module(), keyword()) :: keyword()
  def dynamic_pipeline_opts(source, backend, pipeline, extra_args) do
    key = {source.id, backend.id}
    idle_ms = Application.fetch_env!(:logflare, :scale_to_zero_idle_ms)

    [
      name: Backends.via_source(source, pipeline, backend),
      pipeline: pipeline,
      pipeline_args: [source: source, backend: backend] ++ extra_args,
      min_pipelines: 0,
      max_pipelines: 1,
      initial_count: 1,
      resolve_count: fn state ->
        pending = IngestEventQueue.queues_pending_size(key)
        last_active = IngestEventQueue.last_active(key)
        Backends.resolve_idle_count(state, pending, last_active, idle_ms)
      end
    ]
  end
end
