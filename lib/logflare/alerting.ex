defmodule Logflare.Alerting do
  @moduledoc """
  The Alerting context.
  """

  import Ecto.Query, warn: false

  alias Logflare.Alerting.AlertQuery
  alias Logflare.Alerting.AlertWorker
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

  require Logger
  require OpenTelemetry.Tracer

  @future_job_states ~w(available scheduled executing)

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
  Lists all alert queries across all users.
  Used by the scheduler worker to enqueue jobs.
  """
  @spec list_all_alert_queries() :: [AlertQuery.t()]
  def list_all_alert_queries do
    from(q in AlertQuery, where: q.enabled == true)
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
    |> handle_enabled_change(alert_query)
  end

  defp handle_enabled_change({:ok, %AlertQuery{} = updated} = result, previous) do
    if updated.enabled != previous.enabled do
      if updated.enabled do
        schedule_alert(updated)
      else
        delete_future_alert_jobs(updated.id)
      end
    end

    result
  end

  defp handle_enabled_change(error, _previous), do: error

  @doc """
  Deletes a alert_query.

  ## Examples

      iex> delete_alert_query(alert_query)
      {:ok, %AlertQuery{}}

      iex> delete_alert_query(alert_query)
      {:error, %Ecto.Changeset{}}

  """
  @spec delete_alert_query(AlertQuery.t()) :: {:ok, AlertQuery.t()} | {:error, Ecto.Changeset.t()}
  def delete_alert_query(%AlertQuery{id: id} = alert_query) do
    delete_alert_jobs(id)
    Repo.delete(alert_query)
  end

  defp delete_alert_jobs(alert_query_id) do
    from(j in Oban.Job,
      where: j.worker == "Logflare.Alerting.AlertWorker",
      where: fragment("?->>'alert_query_id' = ?", j.args, ^to_string(alert_query_id))
    )
    |> Repo.delete_all()
  end

  @doc """
  Deletes future (available/scheduled/executing) jobs for an alert query.
  """
  @spec delete_future_alert_jobs(integer()) :: {non_neg_integer(), nil | [term()]}
  def delete_future_alert_jobs(alert_query_id) do
    from(j in Oban.Job,
      where: j.worker == "Logflare.Alerting.AlertWorker",
      where: fragment("?->>'alert_query_id' = ?", j.args, ^to_string(alert_query_id)),
      where: j.state in @future_job_states
    )
    |> Repo.delete_all()
  end

  @doc """
  Schedules the next 5 runs for an alert query based on its cron expression.
  Inserts AlertWorker jobs into Oban.
  """
  @spec schedule_alert(AlertQuery.t()) :: :ok
  def schedule_alert(%AlertQuery{} = alert_query) do
    case Crontab.CronExpression.Parser.parse(alert_query.cron) do
      {:ok, cron_expr} ->
        now = NaiveDateTime.utc_now()

        cron_expr
        |> Crontab.Scheduler.get_next_run_dates(now)
        |> Enum.take(5)
        |> Enum.each(fn run_date ->
          scheduled_at = DateTime.from_naive!(run_date, "Etc/UTC")

          %{alert_query_id: alert_query.id, scheduled_at: DateTime.to_iso8601(scheduled_at)}
          |> AlertWorker.new(scheduled_at: scheduled_at)
          |> Oban.insert()
        end)

      {:error, reason} ->
        Logger.warning("Invalid cron expression for alert #{alert_query.id}: #{inspect(reason)}")
    end

    :ok
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
  Performs the check lifecycle of an AlertQuery.

  Send notifications if necessary configurations are set. If no results are returned from the query execution, no alert is sent.
  """
  @spec run_alert(AlertQuery.t() | integer(), :scheduled) ::
          {:ok, map()} | {:error, :not_found | :no_results | any}
  def run_alert(alert_id, :scheduled) when is_integer(alert_id) do
    if alert_query = get_alert_query_by(id: alert_id) do
      run_alert(alert_query, :scheduled)
    else
      {:error, :not_found}
    end
  end

  def run_alert(%AlertQuery{} = alert_query, :scheduled) do
    cluster_size = Cluster.Utils.actual_cluster_size()

    OpenTelemetry.Tracer.with_span "alerting.run_alert", %{
      "alert.id" => alert_query.id,
      "alert.name" => alert_query.name,
      "alert.user_id" => alert_query.user_id,
      "system.cluster_size" => cluster_size
    } do
      run_alert(alert_query)
    end
  end

  @spec run_alert(AlertQuery.t()) :: {:ok, map()} | {:error, :no_results} | {:error, any()}
  def run_alert(%AlertQuery{} = alert_query) do
    alert_query = alert_query |> preload_alert_query()

    case execute_alert_query(alert_query) do
      {:ok, %{rows: [_ | _] = results} = result} ->
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

        {:ok, Map.put(result, :fired, true)}

      {:ok, %{rows: rows} = result} when rows == [] or rows == nil ->
        {:ok, Map.put(result, :fired, false)}

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
  Inserts an immediate AlertWorker job for manual triggering.
  """
  @spec trigger_alert_now(AlertQuery.t()) :: {:ok, Oban.Job.t()} | {:error, Oban.Job.changeset()}
  def trigger_alert_now(%AlertQuery{id: alert_query_id}) do
    %{alert_query_id: alert_query_id, scheduled_at: DateTime.to_iso8601(DateTime.utc_now())}
    |> AlertWorker.new()
    |> Oban.insert()
  end

  @doc """
  Lists recent execution history for an alert query from Oban jobs.
  """
  @spec list_execution_history(integer()) :: [Oban.Job.t()]
  def list_execution_history(alert_query_id) do
    from(j in Oban.Job,
      where: j.worker == "Logflare.Alerting.AlertWorker",
      where: fragment("?->>'alert_query_id' = ?", j.args, ^to_string(alert_query_id)),
      order_by: [desc: j.scheduled_at],
      limit: 50
    )
    |> Repo.all()
  end

  @doc """
  Lists future (upcoming) jobs for an alert query.
  """
  @spec list_future_jobs(integer()) :: [Oban.Job.t()]
  def list_future_jobs(alert_query_id) do
    from(j in Oban.Job,
      where: j.worker == "Logflare.Alerting.AlertWorker",
      where: fragment("?->>'alert_query_id' = ?", j.args, ^to_string(alert_query_id)),
      where: j.state in @future_job_states,
      order_by: [asc: j.scheduled_at]
    )
    |> Repo.all()
  end

  @doc """
  Returns a query for past (completed/failed/cancelled/discarded) jobs for an alert query.
  """
  @spec past_jobs_query(integer()) :: Ecto.Query.t()
  def past_jobs_query(alert_query_id) do
    from(j in Oban.Job,
      where: j.worker == "Logflare.Alerting.AlertWorker",
      where: fragment("?->>'alert_query_id' = ?", j.args, ^to_string(alert_query_id)),
      where: j.state not in @future_job_states,
      order_by: [desc: j.scheduled_at]
    )
  end

  @doc """
  Partitions jobs into future (upcoming) and past (completed/failed) lists.
  """
  @spec partition_jobs_by_time([Oban.Job.t()]) :: %{future: [Oban.Job.t()], past: [Oban.Job.t()]}
  def partition_jobs_by_time(jobs) do
    {future, past} =
      Enum.split_with(jobs, fn job ->
        to_string(job.state) in @future_job_states
      end)

    %{future: future, past: past}
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

        Logger.error("Alert query execution failed with bad response",
          user_id: alert_query.user_id,
          alert_query_id: alert_query.id,
          alert_name: alert_query.name,
          error_string: inspect(error)
        )

        {:error, error}

      {:error, error} ->
        Logger.error("Alert query execution failed with an unknown error",
          user_id: alert_query.user_id,
          alert_query_id: alert_query.id,
          alert_name: alert_query.name,
          error_string: inspect(error)
        )

        {:error, error}
    end
  end

  # helper to get the google project id via env.
  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]
end
