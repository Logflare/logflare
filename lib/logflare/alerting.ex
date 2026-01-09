defmodule Logflare.Alerting do
  @moduledoc """
  The Alerting context.
  """

  import Ecto.Query, warn: false

  alias Logflare.Alerting.AlertQuery
  alias Logflare.Alerting.AlertsScheduler
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.SlackAdaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Cluster
  alias Logflare.Endpoints
  alias Logflare.Google.BigQuery.GenUtils
  alias Logflare.Repo
  alias Logflare.Teams
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.User
  alias Logflare.Utils

  require Logger
  require OpenTelemetry.Tracer

  def to_job_name(%AlertQuery{id: id}), do: to_job_name(id)
  def to_job_name(id) when is_integer(id), do: String.to_atom(Integer.to_string(id))

  @doc """
  Returns the list of alert_queries.

  ## Examples

      iex> list_alert_queries()
      [%AlertQuery{}, ...]

  """

  def list_alert_queries(%User{id: user_id}) do
    list_alert_queries_by_user_id(user_id)
  end

  def list_alert_queries_by_user_id(user_id) do
    from(q in AlertQuery, where: q.user_id == ^user_id)
    |> Repo.all()
  end

  @doc """
  Lists all alert queries a user has access to, including where the user is a team member.
  """
  @spec list_alert_queries_user_access(User.t()) :: [AlertQuery.t()]
  def list_alert_queries_user_access(%User{} = user) do
    AlertQuery
    |> Teams.filter_by_user_access(user)
    |> Repo.all()
  end

  @doc """
  Gets a single alert_query.

  Raises `Ecto.NoResultsError` if the Alert query does not exist.

  ## Examples

      iex> get_alert_query!(123)
      %AlertQuery{}

      iex> get_alert_query!(456)
      ** (Ecto.NoResultsError)

  """
  def get_alert_query!(id), do: Repo.get!(AlertQuery, id)

  def get_alert_query_by(kw) do
    Repo.get_by(AlertQuery, kw)
  end

  @doc """
  Gets an alert query by id that the user has access to.
  Returns the alert query if the user owns it or is a team member, otherwise returns nil.
  """
  @spec get_alert_query_by_user_access(User.t() | TeamUser.t(), integer() | String.t()) ::
          AlertQuery.t() | nil
  def get_alert_query_by_user_access(user_or_team_user, id)
      when is_integer(id) or is_binary(id) do
    AlertQuery
    |> Teams.filter_by_user_access(user_or_team_user)
    |> where([alert_query], alert_query.id == ^id)
    |> Repo.one()
  end

  def preload_alert_query(alert) do
    alert
    |> Repo.preload([:user, :backends])
    |> then(fn %AlertQuery{backends: backends} = alert ->
      %{alert | backends: Enum.map(backends, &Backends.typecast_config_string_map_to_atom_map/1)}
    end)
  end

  @doc """
  Creates a alert_query.

  ## Examples

      iex> create_alert_query(%{field: value})
      {:ok, %AlertQuery{}}

      iex> create_alert_query(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_alert_query(%User{} = user, attrs \\ %{}) do
    user
    |> Ecto.build_assoc(:alert_queries)
    |> Repo.preload(:user)
    |> AlertQuery.changeset(attrs)
    |> Repo.insert()
  end

  @doc """
  Updates a alert_query.

  ## Examples

      iex> update_alert_query(alert_query, %{field: new_value})
      {:ok, %AlertQuery{}}

      iex> update_alert_query(alert_query, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_alert_query(%AlertQuery{} = alert_query, attrs) do
    backends_modified = if backends = Map.get(attrs, :backends), do: true, else: false

    alert_query
    |> preload_alert_query()
    |> AlertQuery.changeset(attrs)
    |> then(fn
      changeset when backends_modified == true ->
        Ecto.Changeset.put_assoc(changeset, :backends, backends)

      changeset ->
        changeset
    end)
    |> Repo.update()
  end

  @doc """
  Deletes a alert_query.

  ## Examples

      iex> delete_alert_query(alert_query)
      {:ok, %AlertQuery{}}

      iex> delete_alert_query(alert_query)
      {:error, %Ecto.Changeset{}}

  """
  def delete_alert_query(%AlertQuery{} = alert_query) do
    with {:ok, _} <- Repo.delete(alert_query),
         {:ok, _job} <- delete_alert_job(alert_query) do
      {:ok, alert_query}
    else
      {:error, :not_found} -> {:ok, alert_query}
    end
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking alert_query changes.

  ## Examples

      iex> change_alert_query(alert_query)
      %Ecto.Changeset{data: %AlertQuery{}}

  """
  @spec change_alert_query(AlertQuery.t()) :: Ecto.Changeset.t()
  def change_alert_query(%AlertQuery{} = alert_query, attrs \\ %{}) do
    AlertQuery.changeset(alert_query, attrs)
  end

  @doc """
  Retrieves a Job based on AlertQuery.
  Job shares the same id as AlertQuery, resulting in a 1-1 relationship.
  """
  @spec get_alert_job(AlertQuery.t()) :: Quantum.Job.t()
  def get_alert_job(%AlertQuery{id: id}), do: get_alert_job(id)

  def get_alert_job(id) do
    on_scheduler_node(fn ->
      AlertsScheduler.find_job(to_job_name(id))
    end)
  end

  @doc """
  Updates or creates a new Quantum.Job based on a given AlertQuery.
  """
  @spec upsert_alert_job(AlertQuery.t()) :: {:ok, Quantum.Job.t()}
  def upsert_alert_job(%AlertQuery{} = alert_query) do
    job = create_alert_job_struct(alert_query)
    :ok = on_scheduler_node(fn -> AlertsScheduler.add_job(job) end)
    {:ok, job}
  end

  @doc """
  Creates an alert job struct (but does not insert it into the scheduler.)
  """
  @spec create_alert_job_struct(AlertQuery.t()) :: Quantum.Job.t()
  def create_alert_job_struct(%AlertQuery{} = alert_query) do
    %AlertQuery{id: alert_query_id, cron: cron} = alert_query

    if is_nil(alert_query_id) do
      raise ArgumentError, "AlertQuery is missing id: #{inspect(alert_query)}"
    end

    AlertsScheduler.new_job(run_strategy: Quantum.RunStrategy.Local)
    |> Quantum.Job.set_task({__MODULE__, :run_alert, [alert_query_id, :scheduled]})
    |> Quantum.Job.set_schedule(Crontab.CronExpression.Parser.parse!(cron))
    |> Quantum.Job.set_name(to_job_name(alert_query))
  end

  @doc """
  Initializes and ensures that all alert jobs are created.
  TODO: batching instead of loading whole table.
  """
  def init_alert_jobs do
    AlertQuery
    |> Repo.all()
    |> Enum.map(fn alert_query ->
      create_alert_job_struct(alert_query)
    end)
  end

  def sync_alert_jobs do
    on_scheduler_node(fn ->
      Utils.Tasks.start_child(&do_sync_alert_jobs/0)
    end)
  end

  defp do_sync_alert_jobs do
    init_alert_jobs()
    |> tap(fn _ -> AlertsScheduler.delete_all_jobs() end)
    |> Enum.each(&AlertsScheduler.add_job/1)
  end

  @doc """
  Syncs a specific alert job by alert_id.
  Upserts the job if it doesn't exist, otherwise deletes the existing job.
  """
  @spec sync_alert_job(integer) :: :ok | {:error, :not_found}
  def sync_alert_job(alert_id) when is_integer(alert_id) do
    on_scheduler_node(fn -> do_sync_alert_job(alert_id) end)
  end

  defp do_sync_alert_job(alert_id) do
    if alert_query = get_alert_query_by(id: alert_id) do
      job = create_alert_job_struct(alert_query)
      :ok = AlertsScheduler.add_job(job)
    else
      AlertsScheduler.delete_job(to_job_name(alert_id))
      {:error, :not_found}
    end
  end

  @doc """
  Performs the check lifecycle of an AlertQuery.

  Send notifications if necessary configurations are set. If no results are returned from the query execution, no alert is sent.
  """
  @spec run_alert(AlertQuery.t() | integer(), :scheduled) ::
          :ok | {:error, :not_enabled | :not_found | :no_results | :below_min_cluster_size | any}
  def run_alert(alert_id, :scheduled) when is_integer(alert_id) do
    if alert_query = get_alert_query_by(id: alert_id) do
      run_alert(alert_query, :scheduled)
    else
      AlertsScheduler.delete_job(to_job_name(alert_id))
      {:error, :not_found}
    end
  end

  def run_alert(%AlertQuery{} = alert_query, :scheduled) do
    # perform pre-run checks
    cfg = Application.get_env(:logflare, Logflare.Alerting)
    cluster_size = Cluster.Utils.actual_cluster_size()

    cond do
      cfg[:enabled] == false ->
        {:error, :not_enabled}

      cfg[:min_cluster_size] >= cluster_size ->
        {:error, :below_min_cluster_size}

      true ->
        OpenTelemetry.Tracer.with_span "alerting.run_alert", %{
          "alert.id" => alert_query.id,
          "alert.name" => alert_query.name,
          "alert.user_id" => alert_query.user_id,
          "system.cluster_size" => cluster_size
        } do
          run_alert(alert_query)
        end
    end
  end

  @spec run_alert(AlertQuery.t()) :: :ok | {:error, :no_results} | {:error, any()}
  def run_alert(%AlertQuery{} = alert_query) do
    alert_query = alert_query |> preload_alert_query()

    case execute_alert_query(alert_query) do
      {:ok, %{rows: [_ | _] = results}} ->
        if alert_query.webhook_notification_url do
          send_webhook_notification(alert_query, results)
        end

        if alert_query.slack_hook_url do
          send_slack_notification!(alert_query, results)
        end

        # iterate over backends and fire for each
        for backend <- alert_query.backends do
          adaptor_mod = Adaptor.get_adaptor(backend)
          adaptor_mod.send_alert(backend, alert_query, results)

          OpenTelemetry.Tracer.add_event(
            "alerting.run_alert.#{backend.type}.notification_sent",
            %{
              "alert.backend.id" => backend.id,
              "alert.backend.type" => backend.type
            }
          )
        end

        :ok

      {:ok, %{rows: []}} ->
        {:error, :no_results}

      {:ok, %{rows: nil}} ->
        {:error, :no_results}

      other ->
        other
    end
  end

  defp send_webhook_notification(alert_query, results) do
    WebhookAdaptor.Client.send(
      url: alert_query.webhook_notification_url,
      body: %{
        "result" => results
      }
    )

    OpenTelemetry.Tracer.add_event("alerting.run_alert.webhook_notification_sent", %{})
  end

  defp send_slack_notification!(alert_query, results) do
    {:ok, res} = SlackAdaptor.send_message(alert_query, results)

    if res.status != 200 do
      Logger.warning(
        "SlackAdaptor send_message failed with #{res.status} : #{inspect(res.body)}",
        error_string: inspect(res)
      )
    end

    OpenTelemetry.Tracer.add_event("alerting.run_alert.slack_notification_sent", %{})
  end

  @doc """
  Deletes an AlertQuery's job from the scheduler
  noop if already deleted.
  ### Examples

  ```elixir
  iex> delete_alert_job(%AlertQuery{})
  :ok
  iex> delete_alert_job(alert_query.id)
  :ok
  ```
  """
  @spec delete_alert_job(AlertQuery.t() | number()) ::
          {:ok, Quantum.Job.t()} | {:error, :not_found}
  def delete_alert_job(%AlertQuery{id: id}), do: delete_alert_job(id)

  def delete_alert_job(alert_id) when is_integer(alert_id) do
    on_scheduler_node(fn ->
      case AlertsScheduler.find_job(to_job_name(alert_id)) do
        %_{} = job ->
          AlertsScheduler.delete_job(job.name)
          {:ok, job}

        nil ->
          {:error, :not_found}
      end
    end)
  end

  @doc """
  List alert jobs on the scheduler
  """
  def list_alert_jobs do
    on_scheduler_node(fn ->
      AlertsScheduler.jobs()
    end)
  end

  @spec on_scheduler_node((-> func_ret)) :: func_ret when func_ret: term
  defp on_scheduler_node(func) do
    case GenServer.whereis(scheduler_name()) do
      pid when is_pid(pid) ->
        pid
        |> node()
        |> Cluster.Utils.rpc_call(func)

      {_name, node} ->
        Cluster.Utils.rpc_call(node, func)

      nil ->
        raise "Alerting scheduler node not found"
    end
  end

  @doc """
  Executes an AlertQuery and returns its results

  Requires `:user` key to be preloaded.

  ### Examples

  ```elixir
  iex> execute_alert_query(alert_query)
  {:ok, [%{"user_id" => "my-user-id"}]}
  ```
  """
  @spec execute_alert_query(AlertQuery.t(), use_query_cache: boolean) ::
          Logflare.BqRepo.query_result() | {:error, any()}
  def execute_alert_query(%AlertQuery{user: %User{}} = alert_query, opts \\ []) do
    Logger.debug("Executing AlertQuery | #{alert_query.name} | #{alert_query.id}")

    endpoints = Endpoints.list_endpoints_by(user_id: alert_query.user_id)
    use_query_cache = Keyword.get(opts, :use_query_cache, true)

    alerts =
      list_alert_queries_by_user_id(alert_query.user_id)
      |> Enum.filter(&(&1.id != alert_query.id))

    with {:ok, expanded_query} <-
           Logflare.Sql.expand_subqueries(
             alert_query.language,
             alert_query.query,
             endpoints ++ alerts
           ),
         {:ok, transformed_query} <-
           Logflare.Sql.transform(alert_query.language, expanded_query, alert_query.user_id),
         {:ok, result} <-
           BigQueryAdaptor.execute_query(
             {
               alert_query.user.bigquery_project_id || env_project_id(),
               alert_query.user.bigquery_dataset_id,
               alert_query.user.id
             },
             {transformed_query, []},
             parameterMode: "NAMED",
             maxResults: 1000,
             location: alert_query.user.bigquery_dataset_location,
             use_query_cache: use_query_cache,
             labels: %{
               "alert_id" => alert_query.id
             },
             query_type: :alerts
           ) do
      {:ok, result}
    else
      {:error, %Tesla.Env{body: body}} ->
        error =
          Jason.decode!(body)["error"]
          |> GenUtils.process_bq_errors(alert_query.user_id)
          |> case do
            %{"message" => msg} -> msg
            other -> other
          end

        {:error, error}

      err ->
        err
    end
  end

  # helper to get the google project id via env.
  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]

  @doc """
  Returns the alerts scheduler :via name used for syn registry.
  """
  def scheduler_name do
    ts = System.os_time(:nanosecond)
    # add nanosecond resolution for timestamp comparison
    {:via, :syn, {:alerting, Logflare.Alerting.AlertsScheduler, %{timestamp: ts}}}
  end
end
