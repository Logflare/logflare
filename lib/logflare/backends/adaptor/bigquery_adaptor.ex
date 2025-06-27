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
  alias Logflare.Google.CloudResourceManager
  use Supervisor
  require Logger

  @behaviour Logflare.Backends.Adaptor
  @service_account_prefix "logflare-managed"
  @managed_service_account_partition_count 5
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

  @doc """
  Returns the email of a managed service account

    iex> managed_service_account_name("my-project", 0)
    "logflare-managed-0@my-project.iam.gserviceaccount.com"
  """
  @spec managed_service_account_name(String.t(), non_neg_integer()) :: String.t()
  def managed_service_account_name(project_id, service_account_index \\ 0) do
    "#{managed_service_account_id(service_account_index)}@#{project_id}.iam.gserviceaccount.com"
  end

  @doc """
  Returns the id of a managed service account

    iex> managed_service_account_id("my-project", 0)
    "logflare-managed-0"
  """
  @spec managed_service_account_id(non_neg_integer()) :: String.t()
  def managed_service_account_id(service_account_index \\ 0) do
    "#{@service_account_prefix}-#{service_account_index}"
  end

  @doc """
  Lists all managed service accounts.

    iex> list_managed_service_accounts()
    [%GoogleApi.IAM.V1.Model.ServiceAccount{...}, ...]


    https://hexdocs.pm/google_api_iam/0.45.0/GoogleApi.IAM.V1.Model.ServiceAccount.html
  """
  @spec list_managed_service_accounts(String.t()) :: [GoogleApi.IAM.V1.Model.ServiceAccount.t()]
  def list_managed_service_accounts(project_id \\ nil) do
    project_id = project_id || Application.get_env(:logflare, Logflare.Google)[:project_id]

    get_next_page(project_id, nil)
    |> Enum.filter(&(&1.name =~ @service_account_prefix))
  end

  defp handle_response({:ok, response}, project_id) do
    case response do
      %{accounts: accounts, nextPageToken: nil} ->
        accounts

      %{accounts: accounts, nextPageToken: next_page_token} ->
        get_next_page(project_id, next_page_token) ++ accounts
    end
    |> List.flatten()
  end

  defp handle_response({:error, error}, _project_id) do
    Logger.error("Error listing managed service accounts: #{inspect(error)}")
    []
  end

  # handles pagination for the IAM api
  defp get_next_page(project_id, page_token) do
    GenUtils.get_conn(:default)
    |> GoogleApi.IAM.V1.Api.Projects.iam_projects_service_accounts_list("projects/#{project_id}",
      pageSize: 100,
      pageToken: page_token
    )
    |> handle_response(project_id)
  end

  @doc """
  Creates managed service accounts for the project. Multiple service accounts are created, each with partitioning.

    iex> create_managed_service_accounts()
    :ok
  """
  @spec create_managed_service_accounts(String.t()) :: [
          GoogleApi.IAM.V1.Model.ServiceAccount.t()
        ]
  def create_managed_service_accounts(project_id \\ nil) do
    project_id = project_id || Application.get_env(:logflare, Logflare.Google)[:project_id]

    # determine the ids of of service accounts to create, based on what service accounts already exist
    size =
      Application.get_env(:logflare, :bigquery_backend_adaptor)[
        :managed_service_account_pool_size
      ]

    accounts =
      if size > 0 do
        existing = list_managed_service_accounts(project_id) |> Enum.map(& &1.email)

        indexes =
          for i <- 0..(size - 1),
              managed_service_account_name(project_id, i) not in existing,
              do: i

        for i <- indexes, {:ok, sa} = create_managed_service_account(project_id, i) do
          sa
        end
      else
        []
      end

    {:ok, accounts}
  end

  defp create_managed_service_account(project_id, service_account_index) do
    GenUtils.get_conn(:default)
    |> GoogleApi.IAM.V1.Api.Projects.iam_projects_service_accounts_create(
      "projects/#{project_id}",
      body: %GoogleApi.IAM.V1.Model.CreateServiceAccountRequest{
        accountId: managed_service_account_id(service_account_index)
      }
    )
  end

  @doc """
  Returns the size of the managed service account pool from configuration

    iex> managed_service_account_pool_size()
    5
  """
  def managed_service_account_pool_size do
    Application.get_env(:logflare, :bigquery_backend_adaptor)[:managed_service_account_pool_size]
  end

  @doc """
  Returns the number of partitions for each managed service account

    iex> managed_service_account_partition_count()
    #{@managed_service_account_partition_count}
  """
  def managed_service_account_partition_count, do: @managed_service_account_partition_count

  @doc """
  Returns the number of partitions for the ingest service account, which accounts for number of schedulers.

    iex> ingest_service_account_partition_count()
    5
  """
  def ingest_service_account_partition_count,
    do: max(@managed_service_account_partition_count, System.schedulers_online())

  # Goth provisioning

  @doc """
  Returns a child spec for the Goth PartitionSupervisor, which is partitioned for each service account.

    iex> partitioned_goth_child_spec()
    {PartitionSupervisor, ...}

  if no base service account is set, no child spec is returned.
  """
  @spec partitioned_goth_child_spec() :: Supervisor.child_spec() | nil
  def partitioned_goth_child_spec() do
    if json = Application.get_env(:goth, :json) do
      {PartitionSupervisor,
       child_spec: goth_child_spec(json),
       name: Logflare.GothPartitionSup,
       with_arguments: fn [opts], partition ->
         [Keyword.put(opts, :name, {Logflare.Goth, partition})]
       end}
    end
  end

  @doc """
  Returns a Goth child spec for a given service account key. If `sub` is provided, the tokens generated will be impersonated by the `sub` service account.

    iex> goth_child_spec(json)
    {Goth, ...}
  """
  @spec goth_child_spec(String.t(), String.t()) :: Supervisor.child_spec()
  def goth_child_spec(json, sub \\ nil) do
    credentials = Jason.decode!(json)
    source = {:service_account, credentials, if(sub, do: [sub: sub], else: [])}

    {
      Goth,
      # https://hexdocs.pm/goth/Goth.html#fetch/2
      #  refresh 15 min before
      #  don't start server until fetch is made
      #  cap retries at 10s, warn when >5
      name: Logflare.Goth,
      source: source,
      refresh_before: 60 * 15,
      prefetch: :sync,
      http_client: &goth_finch_http_client/1,
      retry_delay: fn
        n when n < 3 ->
          1000

        n when n < 5 ->
          Logger.warning("Goth refresh retry count is #{n}")
          1000 * 3

        n when n < 10 ->
          Logger.warning("Goth refresh retry count is #{n}")
          1000 * 5

        n ->
          Logger.warning("Goth refresh retry count is #{n}")
          1000 * 10
      end
    }
  end

  @doc """
  Returns a list of partitioned Goth child specs with impersonation for the set service account.

    iex> impersonated_goth_child_specs()
    [{PartitionSupervisor, ...}, ...]
  """
  def impersonated_goth_child_specs() do
    project_id = Application.get_env(:logflare, Logflare.Google)[:project_id]
    pool_size = managed_service_account_pool_size()
    json = Application.get_env(:goth, :json)

    if json != nil and pool_size > 0 do
      for i <- 0..(pool_size - 1) do
        spec = goth_child_spec(json, managed_service_account_name(project_id, i))

        {PartitionSupervisor,
         child_spec: spec,
         partitions: @managed_service_account_partition_count,
         name: String.to_atom("Logflare.GothPartitionSup_#{i}"),
         with_arguments: fn [opts], partition ->
           [Keyword.put(opts, :name, {Logflare.GothQuery, i, partition})]
         end}
      end
    else
      []
    end
  end

  # tell goth to use our finch pool
  # https://github.com/peburrows/goth/blob/master/lib/goth/token.ex#L144
  defp goth_finch_http_client(options) do
    {method, options} = Keyword.pop!(options, :method)
    {url, options} = Keyword.pop!(options, :url)
    {headers, options} = Keyword.pop!(options, :headers)
    {body, options} = Keyword.pop!(options, :body)

    Finch.build(method, url, headers, body)
    |> Finch.request(Logflare.FinchGoth, options)
  end

  @doc """
  Updates the IAM policy for the project.

    iex> update_iam_policy()
    :ok
  """
  def update_iam_policy() do
    CloudResourceManager.set_iam_policy(async: false)
  end
end
