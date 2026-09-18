defmodule Logflare.Backends.Adaptor.WebhookV2Adaptor do
  @moduledoc """
  Backend adaptor for webhooks / HTTP posts that uses consolidated ingestion.

  All sources attached to the backend share one pipeline per node. This gives larger
  batches, fewer processes, and bounded in-flight events.

  The config is a superset of the `Logflare.Backends.Adaptor.WebhookAdaptor` config.
  Cast, validation, and redaction of the shared fields delegate to that module.

  ### Batch size

  The `:batch_size` option sets the maximum event count of one request.
  """

  @behaviour Logflare.Backends.Adaptor

  use Supervisor

  alias __MODULE__.Pipeline
  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Utils

  @default_batch_size 1_000
  @max_batch_size 10_000
  @pipeline_count 1

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

  @impl Logflare.Backends.Adaptor
  def consolidated_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  @spec start_link(Backend.t()) :: Supervisor.on_start()
  def start_link(%Backend{} = backend) do
    Supervisor.start_link(__MODULE__, backend, name: Backends.via_backend(backend, __MODULE__))
  end

  @impl Supervisor
  def init(%Backend{} = backend) do
    IngestEventQueue.current_generation_tid({:consolidated, backend.id})

    children = [
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
    |> Map.update!(:types, &Map.put(&1, :batch_size, :integer))
    |> Changeset.cast(params, [:batch_size])
    |> Utils.default_field_value(:batch_size, @default_batch_size)
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> WebhookAdaptor.validate_config()
    |> Changeset.validate_number(:batch_size,
      greater_than: 0,
      less_than_or_equal_to: @max_batch_size
    )
  end

  @impl Logflare.Backends.Adaptor
  defdelegate redact_config(config), to: WebhookAdaptor

  @impl Logflare.Backends.Adaptor
  def sanitize_config_for_display(config) do
    config
    |> Map.delete(:batch_size)
    |> WebhookAdaptor.sanitize_config_for_display()
    |> Map.merge(Map.take(config, [:batch_size]))
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
