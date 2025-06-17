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
  alias Logflare.Google.BigQuery.GenUtils
  use Supervisor
  require Logger

  @behaviour Logflare.Backends.Adaptor
  @service_account_prefix "logflare_managed"

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

    any_above_threshold? = Enum.any?(lens_no_startup_values, &(&1 >= 500))

    cond do
      # max out pipelines, overflow risk
      startup_size > 0 ->
        state.pipeline_count + ceil(startup_size / 500)

      any_above_threshold? and len > 0 ->
        state.pipeline_count + ceil(len / 500)

      # gradual decrease
      Enum.all?(lens_no_startup_values, &(&1 < 50)) and len < 500 and state.pipeline_count > 1 and
          (sec_since_last_decr > 60 or state.last_count_decrease == nil) ->
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

  @spec managed_service_account_name(String.t(), non_neg_integer()) :: String.t()
  def managed_service_account_name(project_id, service_account_index \\ 0) do
    "#{@service_account_prefix}_#{service_account_index}@#{project_id}.iam.gserviceaccount.com"
  end


  @doc """
  Lists all managed service accounts
  """
  @spec list_managed_service_accounts(String.t()) :: [GoogleApi.IAM.V1.Model.ServiceAccount.t()]
  def list_managed_service_accounts(project_id) do
    get_next_page(project_id, nil)
    |> Enum.filter(&(&1.name =~ @service_account_prefix))
  end

  defp handle_response({:ok, response}) do
    case response do
      {:ok, %{accounts: accounts, next_page_token: nil}} ->
        accounts

      {:ok, %{accounts: accounts, next_page_token: next_page_token}} ->
        get_next_page(project_id, next_page_token) ++ accounts
    end
    |> List.flatten()
  end

  defp handle_response({:error, error}) do
    Logger.error("Error listing managed service accounts: #{inspect(error)}")
    []
  end

  defp get_next_page(project_id, page_token) do
    GenUtils.get_conn(:default)
    |> GoogleApi.IAM.V1.Api.Projects.iam_projects_service_accounts_list(project_id,
      page_size: 100,
      page_token: page_token
    )
    |> handle_response()
  end

  def create_managed_service_accounts(project_id) do
    # determine the ids of of service accounts to create, based on what service accounts already exist
    size = Application.get_env(:logflare, :bigquery_backend_adaptor)[:managed_service_account_pool_size]
    existing = list_managed_service_accounts(project_id) |> Enum.map(& &1.name)
    indexes = for i <- 0..(size - 1), managed_service_account_name(project_id, i) not in existing, do: i

    for i <- indexes do
      create_managed_service_account(project_id, i)
    end
  end

  defp create_managed_service_account(project_id, service_account_index) do
    GenUtils.get_conn(:default)
    |> GoogleApi.IAM.V1.Api.Projects.iam_projects_service_accounts_create(project_id, %{
      account_id: managed_service_account_name(project_id, service_account_index),
      service_account_object: %{
        project_id: project_id
      }
    })
  end
end
