defmodule Logflare.Backends.Adaptor.BigQueryAdaptor do
  @moduledoc false

  @behaviour Logflare.Backends.Adaptor

  use Supervisor

  import Logflare.Utils.Guards

  require Logger

  alias Ecto.Changeset
  alias Explorer.DataFrame
  alias Logflare.Google.BigQuery.EventUtils
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  alias Logflare.Billing
  alias Logflare.BqRepo
  alias Logflare.Endpoints.Query
  alias Logflare.Google
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Sources.Source.BigQuery.Pipeline
  alias Logflare.Sources.Source.BigQuery.Schema
  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.Users

  @managed_service_account_partition_count 5
  @service_account_prefix "logflare-managed"

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

          Backends.handle_resolve_count(state, lens, source.metrics.avg)
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

  def insert_log_events_via_storage_write_api(log_events, opts) do
    # convert log events to table rows
    opts =
      Keyword.validate!(opts, [:project_id, :dataset_id, :source_token, :source_id, :source_token])

    # get table id
    table_id = format_table_name(opts[:source_token])

    data_frames =
      log_events
      |> Enum.map(&log_event_to_df_struct(&1))
      |> normalize_df_struct_fields()
      |> DataFrame.new()

    # append rows
    GoogleApiClient.append_rows(
      {:arrow, data_frames},
      opts[:project_id],
      opts[:dataset_id],
      table_id
    )
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source_token) when is_atom(source_token) do
    Atom.to_string(source_token)
    |> String.replace("-", "_")
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(%Backend{} = backend, query_string, opts)
      when is_non_empty_binary(query_string) and is_list(opts) do
    execute_query(backend, {query_string, [], %{}}, opts)
  end

  def execute_query(
        %Backend{user_id: user_id},
        {query_string, declared_params, input_params},
        opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) and
             is_list(opts) do
    execute_query_with_context(user_id, query_string, declared_params, input_params, nil, opts)
  end

  def execute_query(
        %Backend{user_id: user_id},
        {query_string, declared_params, input_params, endpoint_query},
        opts
      )
      when is_non_empty_binary(query_string) and is_list(declared_params) and is_map(input_params) and
             is_list(opts) do
    execute_query_with_context(
      user_id,
      query_string,
      declared_params,
      input_params,
      endpoint_query,
      opts
    )
  end

  def execute_query(%Backend{} = _backend, %Ecto.Query{} = _query, _opts) do
    {:error, :not_implemented}
  end

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{project_id: :string, dataset_id: :string}}
    |> Changeset.cast(params, [:project_id, :dataset_id])
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
  Returns a list of all managed service account ids
  """
  def managed_service_account_ids do
    for i <- 0..(managed_service_account_pool_size() - 1),
        do: managed_service_account_id(i)
  end

  @doc """
  Lists all managed service accounts.

    iex> list_managed_service_accounts()
    [%GoogleApi.IAM.V1.Model.ServiceAccount{...}, ...]

    https://hexdocs.pm/google_api_iam/0.45.0/GoogleApi.IAM.V1.Model.ServiceAccount.html
  """
  @spec list_managed_service_accounts(String.t()) :: [GoogleApi.IAM.V1.Model.ServiceAccount.t()]
  def list_managed_service_accounts(project_id \\ nil) do
    project_id = project_id || env_project_id()

    get_next_page(project_id, nil)
    |> Enum.filter(&(&1.name =~ @service_account_prefix))
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
    project_id = project_id || env_project_id()

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

  @doc """
  Returns the size of the managed service account pool from configuration

    iex> managed_service_account_pool_size()
    5
  """
  @spec managed_service_account_pool_size :: integer()
  def managed_service_account_pool_size do
    Application.get_env(:logflare, :bigquery_backend_adaptor)[:managed_service_account_pool_size]
  end

  @doc """
  Returns true if managed service accounts are enabled
  """
  @spec managed_service_accounts_enabled? :: boolean()
  def managed_service_accounts_enabled? do
    managed_service_account_pool_size() > 0
  end

  @doc """
  Returns the number of partitions for each managed service account

    iex> managed_service_account_partition_count()
    #{@managed_service_account_partition_count}
  """
  @spec managed_service_account_partition_count :: integer()
  def managed_service_account_partition_count, do: @managed_service_account_partition_count

  @doc """
  Returns the number of partitions for the ingest service account, which accounts for number of schedulers.

    iex> ingest_service_account_partition_count()
    5
  """
  @spec ingest_service_account_partition_count :: integer()
  def ingest_service_account_partition_count do
    max(managed_service_account_partition_count(), System.schedulers_online())
  end

  # Goth provisioning

  @doc """
  Returns a child spec for the Goth PartitionSupervisor, which is partitioned for each service account.

    iex> partitioned_goth_child_spec()
    {PartitionSupervisor, ...}

  if no base service account is set, no child spec is returned.
  """
  @spec partitioned_goth_child_spec() :: Supervisor.child_spec() | nil
  def partitioned_goth_child_spec do
    if json = Application.get_env(:goth, :json) do
      {PartitionSupervisor,
       child_spec: goth_child_spec(json),
       name: Logflare.GothPartitionSup,
       partitions: ingest_service_account_partition_count(),
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
  @spec impersonated_goth_child_specs :: [Supervisor.child_spec()]
  def impersonated_goth_child_specs do
    project_id = env_project_id()
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

  @doc """
  Updates the IAM policy for the project.

    iex> update_iam_policy()
    :ok
  """
  @spec update_iam_policy :: :ok
  def update_iam_policy(user \\ nil) do
    CloudResourceManager.set_iam_policy(async: false)

    if Map.get(user || %{}, :bigquery_project_id) do
      # byob project, maybe append managed SA to policy
      append_managed_sa_to_iam_policy(user)
    end
  end

  defdelegate get_iam_policy(user), to: CloudResourceManager
  defdelegate append_managed_sa_to_iam_policy(user), to: CloudResourceManager
  defdelegate append_managed_service_accounts(project_id, policy), to: CloudResourceManager
  defdelegate patch_dataset_access(user), to: Google.BigQuery
  defdelegate get_conn(conn_type), to: GenUtils

  defdelegate log_event_to_df_struct(log_event), to: EventUtils
  defdelegate normalize_df_struct_fields(dataframes), to: EventUtils

  # handles pagination for the IAM api
  defp get_next_page(project_id, page_token) do
    GenUtils.get_conn(:default)
    |> GoogleApi.IAM.V1.Api.Projects.iam_projects_service_accounts_list("projects/#{project_id}",
      pageSize: 100,
      pageToken: page_token
    )
    |> handle_api_response(project_id)
  end

  defp handle_api_response({:ok, response}, project_id) do
    case response do
      %{accounts: accounts, nextPageToken: nil} ->
        accounts

      %{accounts: accounts, nextPageToken: next_page_token} ->
        get_next_page(project_id, next_page_token) ++ accounts
    end
    |> List.flatten()
  end

  defp handle_api_response({:error, error}, _project_id) do
    Logger.error("Error listing managed service accounts: #{inspect(error)}")
    []
  end

  @spec create_managed_service_account(project_id :: String.t(), service_account_index :: integer) ::
          {:ok, String.t()} | {:error, String.t()}
  defp create_managed_service_account(project_id, service_account_index) do
    GenUtils.get_conn(:default)
    |> GoogleApi.IAM.V1.Api.Projects.iam_projects_service_accounts_create(
      "projects/#{project_id}",
      body: %GoogleApi.IAM.V1.Model.CreateServiceAccountRequest{
        accountId: managed_service_account_id(service_account_index)
      }
    )
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

  @spec build_bq_params(declared_params :: list(String.t()), input_params :: map()) :: list(map())
  defp build_bq_params(declared_params, input_params) do
    Enum.map(declared_params, fn input_name ->
      %{
        name: input_name,
        parameterValue: %{value: input_params[input_name]},
        parameterType: %{type: "STRING"}
      }
    end)
  end

  @spec build_base_query_opts(user :: User.t(), opts :: Keyword.t()) :: Keyword.t()
  defp build_base_query_opts(%User{bigquery_dataset_location: bigquery_dataset_location}, opts) do
    [
      parameterMode: "NAMED",
      location: bigquery_dataset_location,
      use_query_cache: Keyword.get(opts, :use_query_cache, true),
      dryRun: Keyword.get(opts, :dry_run, false)
    ]
  end

  @spec execute_query_with_context(
          user_id :: integer(),
          query_string :: String.t(),
          declared_params :: [String.t()],
          input_params :: map(),
          nil | Query.t(),
          opts :: Keyword.t()
        ) :: {:ok, Query.t()} | {:error, any()}
  defp execute_query_with_context(user_id, query_string, declared_params, input_params, nil, opts) do
    user = Users.get(user_id)
    bq_params = build_bq_params(declared_params, input_params)
    query_opts = build_base_query_opts(user, opts)

    execute_query(user, query_string, bq_params, query_opts)
  end

  @spec execute_query_with_context(
          user_id :: integer(),
          query_string :: String.t(),
          declared_params :: [String.t()],
          input_params :: map(),
          endpoint_query :: Query.t(),
          opts :: Keyword.t()
        ) :: {:ok, Query.t()} | {:error, any()}
  defp execute_query_with_context(
         user_id,
         query_string,
         declared_params,
         input_params,
         %Query{} = endpoint_query,
         opts
       ) do
    user = Users.get(user_id)
    bq_params = build_bq_params(declared_params, input_params)

    query_opts =
      build_base_query_opts(user, opts) ++
        [
          maxResults: endpoint_query.max_limit,
          labels:
            Map.merge(
              %{"endpoint_id" => endpoint_query.id},
              endpoint_query.parsed_labels || %{}
            )
        ]

    execute_query(user, query_string, bq_params, query_opts)
  end

  @spec execute_query(
          user :: User.t(),
          query_string :: String.t(),
          bq_params :: [map()],
          query_opts :: Keyword.t()
        ) :: {:ok, %{rows: [map()], total_bytes_processed: integer()}} | {:error, any()}
  defp execute_query(%User{} = user, query_string, bq_params, query_opts) do
    case BqRepo.query_with_sql_and_params(
           user,
           user.bigquery_project_id || env_project_id(),
           query_string,
           bq_params,
           query_opts
         ) do
      {:ok, result} ->
        {:ok, %{rows: result.rows, total_bytes_processed: result.total_bytes_processed}}

      {:error, %{body: body}} ->
        error = Jason.decode!(body)["error"] |> GenUtils.process_bq_errors(user.id)
        {:error, error}

      {:error, err} when is_atom(err) ->
        {:error, GenUtils.process_bq_errors(err, user.id)}
    end
  end

  @spec env_project_id :: String.t()
  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]
end
