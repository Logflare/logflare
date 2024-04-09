defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  alias Logflare.Backends
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
      {Pipeline,
       [
         source: source,
         backend: backend,
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

  @doc """
  Returns a ratio representing how full the buffer is. 1.0 for completely full, 0 for completely empty.
  """
  @spec buffer_capacity(integer(), integer()) :: number()
  def buffer_capacity(source_id, backend_id) do
    via = Backends.via_source(source_id, {BufferCounter, backend_id})

    len = BufferCounter.len(via)
    max_len = BufferCounter.get_max_len(via)
    if len == 0, do: 0, else: len / max_len
  end

  @doc """
  Adds an additional Pipeline shard for a given source-backend pair.
  """
  @spec add_shard({Source.t(), Backend.t()}) ::
          :ok | {:error, :max_children} | {:error, {:already_started, pid()}}
  def add_shard({source, backend}) do
    sup_via = Backends.via_source(source, __MODULE__.PipelinesSup, backend.id)

    project_id = backend.config.project_id
    dataset_id = backend.config.dataset_id
    shard_count = DynamicSupervisor.which_children(sup_via) |> Enum.count()

    sup_via
    |> DynamicSupervisor.start_child(
      {Pipeline,
       [
         source: source,
         backend_id: backend.id,
         bigquery_project_id: project_id,
         bigquery_dataset_id: dataset_id,
         name: Backends.via_source(source.id, {Pipeline, backend.id, shard_count + 1})
       ]}
    )
    |> then(fn
      {:ok, _pid} ->
        new_max = (1 + shard_count + 1) * BufferCounter.max_buffer_shard_len()

        Backends.via_source(source.id, {BufferCounter, backend.id})
        |> BufferCounter.set_max_len(new_max)

      err ->
        err
    end)
  end
end
