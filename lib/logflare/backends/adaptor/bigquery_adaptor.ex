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
        max_pipelines: System.schedulers_online(),
        initial_count: 1,
        resolve_interval: 2_500,
        resolve_count: fn state ->
          source = Sources.refresh_source_metrics_for_ingest(source)

          lens = IngestEventQueue.list_pending_counts({source.id, backend.id})

          handle_resolve_count(state, lens, source.metrics.avg)
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
  def handle_resolve_count(state, lens, avg_rate) do
    max_len = Backends.max_ingest_queue_len()

    startup_size =
      Enum.find_value(lens, 0, fn
        {{_sid, _bid, nil}, val} -> val
        _ -> false
      end)

    lens_no_startup =
      Enum.filter(lens, fn
        {{_sid, _bid, nil}, _val} -> false
        _ -> true
      end)

    lens_no_startup_values = Enum.map(lens_no_startup, fn {_, v} -> v end)
    len = Enum.map(lens, fn {_, v} -> v end) |> Enum.sum()

    last_decr = state.last_count_decrease || NaiveDateTime.utc_now()
    sec_since_last_decr = NaiveDateTime.diff(NaiveDateTime.utc_now(), last_decr)

    any_almost_full? = Enum.any?(lens_no_startup_values, &(&1 > 0.5 * max_len))

    cond do
      # max out pipelines, overflow risk
      startup_size > 0 ->
        state.pipeline_count + ceil(startup_size / 5_000)

      any_almost_full? and avg_rate > 5_000 ->
        state.pipeline_count + 2

      any_almost_full? ->
        state.pipeline_count + 1

      # new items incoming
      len > 0 and state.pipeline_count == 0 ->
        1

      # gradual decrease
      Enum.all?(lens_no_startup_values, &(&1 < 0.05 * max_len)) and state.pipeline_count > 1 and
          (sec_since_last_decr > 30 or state.last_count_decrease == nil) ->
        state.pipeline_count - 1

      len == 0 and avg_rate == 0 and
        state.pipeline_count == 1 and
          (sec_since_last_decr > 60 * 5 or state.last_count_decrease == nil) ->
        # scale to zero only if no items for > 5m
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
