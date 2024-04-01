defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Backends.Backend
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Users
  alias Logflare.Billing
  use Supervisor

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

    with :ok <- Backends.register_backend_for_ingest_dispatch(source, backend) do
      children = [
        {BufferCounter,
         [
           source_id: source.id,
           source_token: source.token,
           backend_token: backend.token,
           name: Backends.via_source(source, BufferCounter, backend.id)
         ]},
        {Pipeline,
         [
           source: source,
           backend_id: backend.id,
           bigquery_project_id: project_id,
           bigquery_dataset_id: dataset_id,
           name: Backends.via_source(source, Pipeline, backend.id)
         ]},
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
  end

  @impl Logflare.Backends.Adaptor
  def ingest(_pid, log_events, opts) do
    source_id = Keyword.get(opts, :source_id)
    backend_id = Keyword.get(opts, :backend_id)
    source = Sources.Cache.get_by_id(source_id)

    buffer_counter_via = Backends.via_source(source, {BufferCounter, backend_id})

    messages =
      for le <- log_events,
          do: %Broadway.Message{
            data: le,
            acknowledger: {__MODULE__, buffer_counter_via, nil}
          }

    with {:ok, _count} <- BufferCounter.inc(buffer_counter_via, Enum.count(messages)) do
      Backends.via_source(source, {Pipeline, backend_id})
      |> Broadway.push_messages(messages)
    end

    :ok
  end

  def ack(via, successful, failed) do
    BufferCounter.decr(via, Enum.count(successful) + Enum.count(failed))
    :ok
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
