defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Backends.SourceDispatcher
  alias Logflare.Backends.Backend
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Source.RateCounterServer
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
  def init({source, nil}) do
    # init for when there is no backend

    # maybe use user's bq info

    backend = %Backend{}
    init({source, backend})
  end

  def init({source, backend}) do
    Process.flag(:trap_exit, true)

    user = Users.get(source.user_id)
    plan = Billing.get_plan_by_user(user)

    project_id = backend.config.project_id
    dataset_id = backend.config.dataset_id
    # TODO: remove source_id metadata to reduce confusion
    Logger.metadata(source_id: source.token, source_token: source.token)

    with {:ok, _} <-
           Registry.register(
             SourceDispatcher,
             source.id,
             {__MODULE__, :ingest, backend_id: backend.id, source_id: source.id}
           ) do
      children = [
        {BufferCounter,
         %{
           source_uuid: source.token,
           backend_token: backend.token,
           name: Backends.via_source(source, BufferCounter, backend.id)
         }},
        {Pipeline,
         %{
           source: source,
           bigquery_project_id: project_id,
           bigquery_dataset_id: dataset_id,
           name: Backends.via_source(source, Pipeline, backend.id)
         }},
        {Schema,
         %{
           plan: plan,
           source_id: source.token,
           bigquery_project_id: project_id,
           bigquery_dataset_id: dataset_id
         }},
        {RateCounterServer,
         %{
           source_token: source.token,
           name: Backends.via_source(source, RateCounterServer, backend.id)
         }}
      ]

      Supervisor.init(children, strategy: :one_for_one, max_restarts: 10)
    end
  end

  @impl Logflare.Backends.Adaptor
  def ingest(_pid, log_events, opts) do
    source_id = Keyword.get(opts, :source_id)
    backend_id = Keyword.get(opts, :backend_id)
    source = Sources.Cache.get_by_id(source_id)

    backend =
      if backend_id do
        Backends.Cache.get_backend(backend_id)
      end

    backend_token = if backend, do: backend.token, else: nil

    messages =
      for le <- log_events,
          do: %Broadway.Message{
            data: le,
            acknowledger: {Source.BigQuery.BufferProducer, {source.token, backend_token}, nil}
          }

    BufferCounter.push_batch(source, backend, messages)
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
