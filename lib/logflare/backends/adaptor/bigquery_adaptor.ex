defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.Backend
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Users
  alias Logflare.Sources
  alias Logflare.Billing
  alias Logflare.Backends
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
        min_pipelines: 0,
        max_pipelines: System.schedulers_online() * 2,
        initial_count: 1,
        resolve_count: fn state ->
          source = Sources.refresh_source_metrics_for_ingest(source)
          len = Backends.get_and_cache_local_pending_buffer_len(source.id, backend.id)
          startup_size = IngestEventQueue.get_table_size({source.id, backend.id, nil}) || 0
          handle_resolve_count(state, startup_size, len, source.metrics.avg)
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

  @doc """
  Pipeline count resolution logic, separate to a different functino for easier testing.

  """
  def handle_resolve_count(state, startup_size, len, avg_rate) do
    max_len = Backends.max_buffer_len()
    last_decr = state.last_count_decrease || NaiveDateTime.utc_now()
    sec_since_last_decr = NaiveDateTime.diff(NaiveDateTime.utc_now(), last_decr)

    cond do
      # max out pipelines, overflow risk
      len > max_len / 2 ->
        state.max_pipelines

      # increase based on hardcoded thresholds
      len > max_len / 10 ->
        state.pipeline_count + 5

      len > max_len / 20 ->
        state.pipeline_count + 3

      len > max_len / 50 ->
        state.pipeline_count + 2

      len > max_len / 100 ->
        state.pipeline_count + 1

      startup_size > 0 ->
        state.pipeline_count + 1

      # new items incoming
      len > 0 and state.pipeline_count == 0 ->
        if(len > 500, do: 3, else: 1)

      # gradual decrease
      len < max_len / 100 and state.pipeline_count > 1 and
          (sec_since_last_decr > 30 or state.last_count_decrease == nil) ->
        state.pipeline_count - 1

      len == 0 and avg_rate == 0 and
        state.pipeline_count == 1 and
          (sec_since_last_decr > 150 or state.last_count_decrease == nil) ->
        # scale to zero only if no items for > 5m and incoming rate is 0
        0

      true ->
        state.pipeline_count
    end
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
