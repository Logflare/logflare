defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Backend
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Billing
  alias Logflare.Backends.IngestEventQueue
  use Supervisor
  require Logger

  @behaviour Logflare.Backends.Adaptor

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend} = source_backend) do
    Supervisor.start_link(__MODULE__, source_backend,
      name: Backends.via_source(source, __MODULE__, backend.id)
    )
  end

  @impl true
  def init({source, backend}) do
    backend = backend || %Backend{}

    user = Users.Cache.get(source.user_id)
    plan = Billing.Cache.get_plan_by_user(user)

    project_id = backend.config.project_id
    dataset_id = backend.config.dataset_id
    # TODO: remove source_id metadata to reduce confusion
    Logger.metadata(source_id: source.token, source_token: source.token)

    children = [
      {
        DynamicPipeline,
        # soft limit before a new pipeline is created
        name: Backends.via_source(source, Pipeline, backend.id),
        pipeline: Pipeline,
        pipeline_args: [
          source: source,
          backend: backend,
          bigquery_project_id: project_id,
          bigquery_dataset_id: dataset_id
        ],
        min_pipelines: 1,
        resolve_count: fn ->
          source = Sources.refresh_source_metrics_for_ingest(source)
          len = Backends.local_pending_buffer_len(source, backend)
          # take a factor of the rate * 2
          # if buffer filling up fast, use it instead
          buffer_min = round(len / 1000 * 1.5)

          round(source.metrics.avg / 1000 * 2)
          |> max(buffer_min)
        end
      },
      {Schema,
       [
         plan: plan,
         source: source,
         bigquery_project_id: project_id,
         bigquery_dataset_id: dataset_id,
         name: Backends.via_source(source, Schema, backend.id)
       ]}
    ]

    Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query),
    do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{project_id: :string, dataset_id: :string}}
    |> Ecto.Changeset.cast(params, [:project_id, :dataset_id])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset),
    do: changeset
end
