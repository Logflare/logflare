defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Sources
  alias Logflare.Backends.Backend
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Users
  alias Logflare.Billing
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
      {DynamicPipeline,
         name: Backends.via_source(source, Pipeline, backend.id),
         pipeline: Pipeline,
         max_buffer_len: 10_000,
         pipeline_args: [
          source: source,
          backend: backend,
          bigquery_project_id: project_id,
          bigquery_dataset_id: dataset_id,
         ],
         monitor_callback: &Backends.broadcast_buffer_lens(source.id, backend.id, &1),
         min_pipelines: 1},

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
