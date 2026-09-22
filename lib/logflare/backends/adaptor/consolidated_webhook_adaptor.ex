defmodule Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor do
  @moduledoc """
  Backend adaptor for webhooks / HTTP posts that uses consolidated ingestion.

  All sources attached to the backend share one pipeline per node. This gives larger
  batches, fewer processes, bounded in-flight events, and retries for failed requests.
  A `Logflare.Backends.CircuitBreaker` sheds the retries while the receiver fails.

  The config is a superset of the `Logflare.Backends.Adaptor.WebhookAdaptor` config.
  Cast, validation, and redaction of the shared fields delegate to that module.

  ### Batch size

  The `:batch_size` option sets the maximum event count of one request.

  ### Sampling

  The `:sample_percentage` option sets the percentage of events that the backend
  sends. `pre_ingest/3` keeps each event with that probability and drops the rest
  before they enter the queue. The decision is random and independent for each event.
  """

  @behaviour Logflare.Backends.Adaptor

  use Supervisor

  alias __MODULE__.Pipeline
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.CircuitBreaker
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Sources.Source
  alias Logflare.Utils

  @default_batch_size 1_000
  @max_batch_size 10_000
  @default_sample_percentage 100.0
  @pipeline_count 1
  @display_fields [:batch_size, :sample_percentage]

  @doc false
  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc """
  Default maximum event count of one request.
  """
  @spec default_batch_size() :: pos_integer()
  def default_batch_size, do: @default_batch_size

  @doc """
  Largest `:batch_size` value that the config accepts.
  """
  @spec max_batch_size() :: pos_integer()
  def max_batch_size, do: @max_batch_size

  @doc """
  Default percentage of events that the backend sends.
  """
  @spec default_sample_percentage() :: float()
  def default_sample_percentage, do: @default_sample_percentage

  @impl Logflare.Backends.Adaptor
  def consolidated_ingest?, do: true

  @doc """
  Keeps the configured sample percentage of the events and drops the rest before they
  are queued for the consolidated pipeline.
  """
  @impl Logflare.Backends.Adaptor
  @spec pre_ingest(Source.t(), Backend.t(), [LogEvent.t()]) :: [LogEvent.t()]
  def pre_ingest(_source, _backend, []), do: []

  def pre_ingest(%Source{}, %Backend{} = backend, log_events) do
    case sample_percentage(backend) do
      percentage when percentage >= 100 -> log_events
      percentage -> sample(backend, log_events, percentage)
    end
  end

  @spec sample_percentage(Backend.t()) :: number()
  defp sample_percentage(%Backend{config: %{sample_percentage: percentage}})
       when is_number(percentage),
       do: percentage

  defp sample_percentage(_backend), do: @default_sample_percentage

  @spec sample(Backend.t(), [LogEvent.t()], number()) :: [LogEvent.t()]
  defp sample(backend, log_events, percentage) do
    {kept, dropped} =
      Enum.reduce(log_events, {[], 0}, fn event, {kept, dropped} ->
        if :rand.uniform() * 100 < percentage,
          do: {[event | kept], dropped},
          else: {kept, dropped + 1}
      end)

    emit_sampled_drop(backend, dropped)
    Enum.reverse(kept)
  end

  @spec emit_sampled_drop(Backend.t(), non_neg_integer()) :: :ok
  defp emit_sampled_drop(_backend, 0), do: :ok

  defp emit_sampled_drop(%Backend{id: backend_id, type: backend_type}, dropped) do
    :telemetry.execute(
      [:logflare, :logs, :ingest_logs, :drop_sampled],
      %{count: dropped},
      %{backend_id: backend_id, backend_type: backend_type}
    )
  end

  @impl Logflare.Backends.Adaptor
  @spec start_link(Backend.t()) :: Supervisor.on_start()
  def start_link(%Backend{} = backend) do
    Supervisor.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @impl Supervisor
  def init(%Backend{} = backend) do
    IngestEventQueue.current_generation_tid({:consolidated, backend.id})

    children = [
      CircuitBreaker.child_spec(backend),
      {
        DynamicPipeline,
        name: Backends.via_backend(backend, Pipeline),
        pipeline: Pipeline,
        pipeline_args: [backend: backend],
        min_pipelines: @pipeline_count,
        max_pipelines: @pipeline_count,
        initial_count: @pipeline_count,
        resolve_count: fn _state -> @pipeline_count end
      }
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params, existing_config \\ %{}) do
    params
    |> WebhookAdaptor.cast_config(existing_config)
    |> Map.update!(:types, &Map.merge(&1, %{batch_size: :integer, sample_percentage: :float}))
    |> Changeset.cast(params, [:batch_size, :sample_percentage])
    |> Utils.default_field_value(:batch_size, @default_batch_size)
    |> Utils.default_field_value(:sample_percentage, @default_sample_percentage)
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> WebhookAdaptor.validate_config()
    |> Changeset.validate_number(:batch_size,
      greater_than: 0,
      less_than_or_equal_to: @max_batch_size
    )
    |> Changeset.validate_number(:sample_percentage,
      greater_than: 0,
      less_than_or_equal_to: 100
    )
  end

  @impl Logflare.Backends.Adaptor
  defdelegate redact_config(config), to: WebhookAdaptor

  @impl Logflare.Backends.Adaptor
  def sanitize_config_for_display(config) do
    config
    |> Map.drop(@display_fields)
    |> WebhookAdaptor.sanitize_config_for_display()
    |> Map.merge(Map.take(config, @display_fields))
  end

  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, atom()}
  def test_connection(%Backend{} = backend) do
    config = Adaptor.get_backend_config(backend)

    WebhookAdaptor.test_connection(
      %{backend | config: Pipeline.put_content_type(config)},
      Pipeline.join_payload(config, [])
    )
  end
end
